#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <semaphore.h>
#include <errno.h>
#define MAX_PROCESSES 64
#define MAX_CHILDREN 64
#define MAX_LINE_LENGTH 256

typedef enum {
    RUNNING,
    BLOCKED,
    ZOMBIE,
    TERMINATED
} ProcessState;

typedef struct PCB {
    int pid;
    int ppid;
    ProcessState state;
    int exit_status;
    int children[MAX_CHILDREN];
    int num_children;
    int has_exited;
    
    pthread_mutex_t wait_mutex;
    pthread_cond_t wait_cond;
    int child_exited;
} PCB;

typedef struct {
    PCB processes[MAX_PROCESSES];
    int count;
    int next_pid;
    pthread_mutex_t table_mutex;
} ProcessTable;

ProcessTable process_table;

pthread_mutex_t monitor_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t monitor_cond = PTHREAD_COND_INITIALIZER;
int table_modified = 0;
int monitor_running = 1;
FILE *snapshot_file = NULL;

void init_process_table(void);
int pm_fork(int parent_pid);
void pm_exit(int pid, int status);
int pm_wait(int parent_pid, int child_pid);
void pm_kill(int pid);
void pm_ps(void);
void print_process_table_to_file(FILE *fp, const char *header);
void *worker_thread(void *arg);
void *monitor_thread(void *arg);
void signal_table_modified(void);

void init_process_table(void) {
    pthread_mutex_init(&process_table.table_mutex, NULL);
    process_table.count = 0;
    process_table.next_pid = 1;
    
    for (int i = 0; i < MAX_PROCESSES; i++) {
        process_table.processes[i].pid = -1;
        process_table.processes[i].ppid = -1;
        process_table.processes[i].state = TERMINATED;
        process_table.processes[i].exit_status = 0;
        process_table.processes[i].num_children = 0;
        process_table.processes[i].has_exited = 0;
        process_table.processes[i].child_exited = 0;
        pthread_mutex_init(&process_table.processes[i].wait_mutex, NULL);
        pthread_cond_init(&process_table.processes[i].wait_cond, NULL);
    }
    
    PCB *init = &process_table.processes[0];
    init->pid = 1;
    init->ppid = 0;
    init->state = RUNNING;
    init->exit_status = 0;
    init->num_children = 0;
    init->has_exited = 0;
    init->child_exited = 0;
    process_table.count = 1;
    process_table.next_pid = 2;
}

PCB* find_process(int pid) {
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table.processes[i].pid == pid && 
            process_table.processes[i].state != TERMINATED) {
            return &process_table.processes[i];
        }
    }
    return NULL;
}

int find_process_index(int pid) {
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table.processes[i].pid == pid && 
            process_table.processes[i].state != TERMINATED) {
            return i;
        }
    }
    return -1;
}

int pm_fork(int parent_pid) {
    pthread_mutex_lock(&process_table.table_mutex);
    
    PCB *parent = find_process(parent_pid);
    if (parent == NULL) {
        pthread_mutex_unlock(&process_table.table_mutex);
        fprintf(stderr, "Error: Parent process %d not found\n", parent_pid);
        return -1;
    }
    
    if (process_table.count >= MAX_PROCESSES) {
        pthread_mutex_unlock(&process_table.table_mutex);
        fprintf(stderr, "Error: Process table full\n");
        return -1;
    }
    
    int slot = -1;
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table.processes[i].state == TERMINATED) {
            slot = i;
            break;
        }
    }
    
    if (slot == -1) {
        pthread_mutex_unlock(&process_table.table_mutex);
        fprintf(stderr, "Error: No empty slot in process table\n");
        return -1;
    }
    
    PCB *child = &process_table.processes[slot];
    child->pid = process_table.next_pid++;
    child->ppid = parent_pid;
    child->state = RUNNING;
    child->exit_status = 0;
    child->num_children = 0;
    child->has_exited = 0;
    child->child_exited = 0;
    pthread_mutex_init(&child->wait_mutex, NULL);
    pthread_cond_init(&child->wait_cond, NULL);
    
    if (parent->num_children < MAX_CHILDREN) {
        parent->children[parent->num_children++] = child->pid;
    }
    
    process_table.count++;
    
    pthread_mutex_unlock(&process_table.table_mutex);
    signal_table_modified();
    
    return child->pid;
}

void pm_exit(int pid, int status) {
    pthread_mutex_lock(&process_table.table_mutex);
    
    PCB *process = find_process(pid);
    if (process == NULL) {
        pthread_mutex_unlock(&process_table.table_mutex);
        fprintf(stderr, "Error: Process %d not found\n", pid);
        return;
    }
    
    if (process->state == ZOMBIE || process->state == TERMINATED) {
        pthread_mutex_unlock(&process_table.table_mutex);
        return;
    }
    
    process->state = ZOMBIE;
    process->exit_status = status;
    process->has_exited = 1;
    
    if (process->ppid > 0) {
        PCB *parent = find_process(process->ppid);
        if (parent != NULL && parent->state != TERMINATED) {
            pthread_mutex_lock(&parent->wait_mutex);
            parent->child_exited = 1;
            pthread_cond_broadcast(&parent->wait_cond);
            pthread_mutex_unlock(&parent->wait_mutex);
        }
    }
    
    pthread_mutex_unlock(&process_table.table_mutex);
    signal_table_modified();
}

int pm_wait(int parent_pid, int child_pid) {
    pthread_mutex_lock(&process_table.table_mutex);
    
    PCB *parent = find_process(parent_pid);
    if (parent == NULL) {
        pthread_mutex_unlock(&process_table.table_mutex);
        fprintf(stderr, "Error: Parent process %d not found\n", parent_pid);
        return -1;
    }
    
    if (parent->num_children == 0) {
        pthread_mutex_unlock(&process_table.table_mutex);
        return 0;
    }
    
    int target_pid = child_pid;
    if (child_pid == -1) {
        target_pid = -1;
        for (int i = 0; i < parent->num_children; i++) {
            PCB *child = find_process(parent->children[i]);
            if (child != NULL && child->state != TERMINATED) {
                target_pid = child->pid;
                break;
            }
        }
        if (target_pid == -1) {
            pthread_mutex_unlock(&process_table.table_mutex);
            return 0;
        }
    }
    
    PCB *child = find_process(target_pid);
    if (child == NULL || child->ppid != parent_pid) {
        pthread_mutex_unlock(&process_table.table_mutex);
        fprintf(stderr, "Error: Child process %d not found or not a child of %d\n", 
                target_pid, parent_pid);
        return -1;
    }
    
    if (child->state == ZOMBIE) {
        child->state = TERMINATED;
        process_table.count--;
        
        for (int i = 0; i < parent->num_children; i++) {
            if (parent->children[i] == target_pid) {
                for (int j = i; j < parent->num_children - 1; j++) {
                    parent->children[j] = parent->children[j + 1];
                }
                parent->num_children--;
                break;
            }
        }
        
        pthread_mutex_unlock(&process_table.table_mutex);
        signal_table_modified();
        return target_pid;
    }
    
    parent->state = BLOCKED;
    pthread_mutex_unlock(&process_table.table_mutex);
    signal_table_modified();
    
    pthread_mutex_lock(&parent->wait_mutex);
    while (!parent->child_exited) {
        pthread_cond_wait(&parent->wait_cond, &parent->wait_mutex);
    }
    parent->child_exited = 0;
    pthread_mutex_unlock(&parent->wait_mutex);
    
    pthread_mutex_lock(&process_table.table_mutex);
    
    int zombie_pid = -1;
    if (child_pid == -1) {
        for (int i = 0; i < parent->num_children; i++) {
            PCB *c = find_process(parent->children[i]);
            if (c != NULL && c->state == ZOMBIE) {
                zombie_pid = c->pid;
                c->state = TERMINATED;
                process_table.count--;
                break;
            }
        }
    } else {
        PCB *c = find_process(child_pid);
        if (c != NULL && c->state == ZOMBIE) {
            zombie_pid = c->pid;
            c->state = TERMINATED;
            process_table.count--;
        }
    }
    
    if (zombie_pid != -1) {
        for (int i = 0; i < parent->num_children; i++) {
            if (parent->children[i] == zombie_pid) {
                for (int j = i; j < parent->num_children - 1; j++) {
                    parent->children[j] = parent->children[j + 1];
                }
                parent->num_children--;
                break;
            }
        }
    }
    
    parent->state = RUNNING;
    pthread_mutex_unlock(&process_table.table_mutex);
    signal_table_modified();
    
    return zombie_pid;
}

void pm_kill(int pid) {
    if (pid <= 0) {
        fprintf(stderr, "Error: Invalid PID %d\n", pid);
        return;
    }
    
    pm_exit(pid, 0);
}

void pm_ps(void) {
    pthread_mutex_lock(&process_table.table_mutex);
    
    printf("PID\tPPID\tSTATE\t\tEXIT_STATUS\n");
    printf("----------------------------------------------\n");
    
    for (int i = 0; i < MAX_PROCESSES; i++) {
        PCB *p = &process_table.processes[i];
        if (p->state != TERMINATED) {
            const char *state_str;
            switch (p->state) {
                case RUNNING: state_str = "RUNNING"; break;
                case BLOCKED: state_str = "BLOCKED"; break;
                case ZOMBIE: state_str = "ZOMBIE"; break;
                default: state_str = "UNKNOWN"; break;
            }
            
            if (p->state == ZOMBIE) {
                printf("%d\t%d\t%s\t%d\n", p->pid, p->ppid, state_str, p->exit_status);
            } else {
                printf("%d\t%d\t%s\t-\n", p->pid, p->ppid, state_str);
            }
        }
    }
    
    pthread_mutex_unlock(&process_table.table_mutex);
}

void print_process_table_to_file(FILE *fp, const char *header) {
    fprintf(fp, "%s\n", header);
    fprintf(fp, "PID\tPPID\tSTATE\t\tEXIT_STATUS\n");
    fprintf(fp, "----------------------------------------------\n");
    
    pthread_mutex_lock(&process_table.table_mutex);
    
    for (int i = 0; i < MAX_PROCESSES; i++) {
        PCB *p = &process_table.processes[i];
        if (p->state != TERMINATED) {
            const char *state_str;
            switch (p->state) {
                case RUNNING: state_str = "RUNNING"; break;
                case BLOCKED: state_str = "BLOCKED"; break;
                case ZOMBIE: state_str = "ZOMBIE"; break;
                default: state_str = "UNKNOWN"; break;
            }
            
            if (p->state == ZOMBIE) {
                fprintf(fp, "%d\t%d\t%s\t%d\n", p->pid, p->ppid, state_str, p->exit_status);
            } else {
                fprintf(fp, "%d\t%d\t%s\t-\n", p->pid, p->ppid, state_str);
            }
        }
    }
    
    pthread_mutex_unlock(&process_table.table_mutex);
    fprintf(fp, "\n");
    fflush(fp);
}

void signal_table_modified(void) {
    pthread_mutex_lock(&monitor_mutex);
    table_modified = 1;
    pthread_cond_signal(&monitor_cond);
    pthread_mutex_unlock(&monitor_mutex);
}

void *monitor_thread(void *arg) {
    (void)arg;
    
    snapshot_file = fopen("snapshots.txt", "w");
    if (snapshot_file == NULL) {
        fprintf(stderr, "Error: Could not open snapshots.txt for writing\n");
        return NULL;
    }
    
    print_process_table_to_file(snapshot_file, "Initial Process Table");
    
    while (1) {
        pthread_mutex_lock(&monitor_mutex);        
        while (!table_modified && monitor_running) {
            pthread_cond_wait(&monitor_cond, &monitor_mutex);
        }
        
        if (!monitor_running) {
            pthread_mutex_unlock(&monitor_mutex);
            break;
        }
        
        table_modified = 0;
        pthread_mutex_unlock(&monitor_mutex);
        usleep(10000);
    }
    
    fclose(snapshot_file);
    return NULL;
}


void *worker_thread(void *arg) {
    char *filename = (char *)arg;
    int thread_id = *((int *)arg);
    extern char **script_files;
    filename = script_files[thread_id];
    
    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        fprintf(stderr, "Error: Could not open script file %s\n", filename);
        return NULL;
    }
    
    char line[MAX_LINE_LENGTH];
    char header[256]; 
    while (fgets(line, sizeof(line), fp) != NULL) {
        line[strcspn(line, "\n")] = 0;
        if (strlen(line) == 0) continue;
        
        char cmd[16];
        int arg1, arg2;
        int num_args = sscanf(line, "%s %d %d", cmd, &arg1, &arg2);
        
        if (num_args < 1) continue;
        snprintf(header, sizeof(header), "Thread %d calls pm_%s %s", 
                 thread_id, cmd, line + strlen(cmd) + 1);
        
        if (strcmp(cmd, "fork") == 0 && num_args >= 2) {
            int child_pid = pm_fork(arg1);
            if (child_pid > 0) {
                pthread_mutex_lock(&monitor_mutex);
                if (snapshot_file != NULL) {
                    print_process_table_to_file(snapshot_file, header);
                }
                pthread_mutex_unlock(&monitor_mutex);
            }
        }
        else if (strcmp(cmd, "exit") == 0 && num_args >= 3) {
            pm_exit(arg1, arg2);
            pthread_mutex_lock(&monitor_mutex);
            if (snapshot_file != NULL) {
                print_process_table_to_file(snapshot_file, header);
            }
            pthread_mutex_unlock(&monitor_mutex);
        }
        else if (strcmp(cmd, "wait") == 0 && num_args >= 3) {
            pm_wait(arg1, arg2);
            pthread_mutex_lock(&monitor_mutex);
            if (snapshot_file != NULL) {
                print_process_table_to_file(snapshot_file, header);
            }
            pthread_mutex_unlock(&monitor_mutex);
        }
        else if (strcmp(cmd, "kill") == 0 && num_args >= 2) {
            pm_kill(arg1);
            pthread_mutex_lock(&monitor_mutex);
            if (snapshot_file != NULL) {
                print_process_table_to_file(snapshot_file, header);
            }
            pthread_mutex_unlock(&monitor_mutex);
        }
        else if (strcmp(cmd, "sleep") == 0 && num_args >= 2) {
            usleep(arg1 * 1000);
        }
    }
    
    fclose(fp);
    return NULL;
}

char **script_files = NULL;

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <script_file1> [script_file2] ...\n", argv[0]);
        fprintf(stderr, "Example: %s thread0.txt thread1.txt thread2.txt\n", argv[0]);
        return 1;
    }
    
    int num_threads = argc - 1;
    script_files = &argv[1];
    init_process_table();
    pthread_t monitor_tid;
    if (pthread_create(&monitor_tid, NULL, monitor_thread, NULL) != 0) {
        fprintf(stderr, "Error: Failed to create monitor thread\n");
        return 1;
    }
    
    usleep(50000);
    pthread_t *worker_tids = malloc(num_threads * sizeof(pthread_t));
    int *thread_ids = malloc(num_threads * sizeof(int));
    
    if (worker_tids == NULL || thread_ids == NULL) {
        fprintf(stderr, "Error: Memory allocation failed\n");
        return 1;
    }
    
    for (int i = 0; i < num_threads; i++) {
        thread_ids[i] = i;
        if (pthread_create(&worker_tids[i], NULL, worker_thread, &thread_ids[i]) != 0) {
            fprintf(stderr, "Error: Failed to create worker thread %d\n", i);
            return 1;
        }
    }
    
    for (int i = 0; i < num_threads; i++) {
        pthread_join(worker_tids[i], NULL);
    }

    usleep(100000);
    pthread_mutex_lock(&monitor_mutex);
    monitor_running = 0;
    pthread_cond_signal(&monitor_cond);
    pthread_mutex_unlock(&monitor_mutex);
    pthread_join(monitor_tid, NULL);
    printf("\nFinal Process Table:\n");
    pm_ps();
    free(worker_tids);
    free(thread_ids);
    printf("\nSimulation complete. Snapshots saved to snapshots.txt\n");
    return 0;
}
