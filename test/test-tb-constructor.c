/* Test construction/destruction functionality */
#include "constructor_wrapper.h"
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

int do_print = 0;
int fd_out = 0;
int pos = 0;
char order[] = "XXXX";

#ifdef ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS(construct_fn)
#endif
ACCRE_DEFINE_CONSTRUCTOR(construct_fn)
#ifdef ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS(construct_fn)
#endif

#ifdef ACCRE_DESTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_PREPRAGMA_ARGS(destruct_fn)
#endif
ACCRE_DEFINE_DESTRUCTOR(destruct_fn)
#ifdef ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS(destruct_fn)
#endif

static void construct_fn() {
    order[pos++] = '1';
}

static void destruct_fn() {
    order[pos++] = '3';
    if (do_print) {
        write(fd_out, order, sizeof(order));
        write(fd_out, "\n", 1);
    }
}

int main(int argc, const char ** argv) {
    order[pos++] = '2';
    if (argc > 1) {
        do_print = 1;
    } else {
        int fd[2];
        if (pipe(fd))
            return 1;
        pid_t pid = fork();
        if (pid < 0) {
            return 1;
        } else if (pid == 0) {
            close(0);
            close(fd[0]);
            dup2(fd[1], 0);
            execl(argv[0], argv[0], "1", (char *) NULL);
            _exit(EXIT_FAILURE);
        } else {
            close(fd[1]);
            int status;
            waitpid(pid, &status, 0);
            if (status)
                return status;
            char wanted[] = "123X";
            char buf[sizeof(wanted) + 1];
            int flags = fcntl(fd[0], F_GETFL, 0);
            fcntl(fd[0], F_SETFL, flags | O_NONBLOCK);
            ssize_t bytes = read(fd[0], buf, sizeof(wanted) - 1);
            if (bytes != sizeof(wanted) - 1)
                return 1;
            printf("Expected: %s\n", wanted);
            printf("Received: %s\n", buf);
            return ((strncmp(buf, wanted, sizeof(wanted) - 1) == 0) ? 0 : 1);
        }
    }
    return 0;
}
