#ifndef DAGGER_JL_H
#define DAGGER_JL_H

#include <stdio.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <julia.h>

enum dagger_type_s {
    dagger_type_bool,
    dagger_type_int8,
    dagger_type_uint8,
    dagger_type_int16,
    dagger_type_uint16,
    dagger_type_int32,
    dagger_type_uint32,
    dagger_type_int64,
    dagger_type_uint64,
    dagger_type_float32,
    dagger_type_float64,
    dagger_type_any
};
typedef enum dagger_type_s dagger_type_t;

struct dagger_spec_s {
    int id;
    void *fn;
    dagger_type_t rettype;
    dagger_type_t *argtypes;
    int nargs;
};
typedef struct dagger_spec_s dagger_spec_t;

struct dagger_task_s {
    jl_value_t *value;
    jl_value_t **preserved;
};
typedef struct dagger_task_s dagger_task_t;

#define REGISTER_JLFUNC(func_name, func_sig) \
    void *func_name##_jlfunc

static jl_value_t *(*dagger_spawn_jlfunc)(dagger_spec_t *spec, jl_value_t **args) = NULL;
static jl_value_t *(*dagger_fetch_jlfunc)(jl_value_t *task) = NULL;
static void (*dagger_wait_jlfunc)(jl_value_t *task) = NULL;

void strappend(char *str, char x)
{
    *(str + strlen(str)) = x;
}
void *daggerc_register_func(const char *jlfunc, const char *jlrettype, int nargs, ...)
{
    char *cfunc_str_head = (char *)calloc(1024, 1);
    char *cfunc_str = cfunc_str_head;
    strcat(cfunc_str, "@cfunction(");
    strcat(cfunc_str, jlfunc);
    strappend(cfunc_str, ',');
    strcat(cfunc_str, jlrettype);
    strappend(cfunc_str, ',');
    strappend(cfunc_str, '(');
    va_list jlargtypes;
    va_start(jlargtypes, nargs);
    for (int i = 0; i < nargs; i++) {
        strcat(cfunc_str, va_arg(jlargtypes, const char *));
        strappend(cfunc_str, ',');
    }
    strappend(cfunc_str, ')');
    strappend(cfunc_str, ')');

    printf("%s\n", cfunc_str_head);
    jl_value_t *func_ptr = jl_eval_string(cfunc_str_head);
    if (jl_exception_occurred()) {
        jl_static_show(1, jl_exception_occurred());
        jl_printf(1, "\n");
        //jlbacktrace();
        printf("Failed to load cfunction pointer: %s\n", jl_typeof_str(jl_exception_occurred()));
        exit(EXIT_FAILURE);
    }
    return jl_unbox_voidpointer(func_ptr);
}

#define GENERATE_JLFUNC(func_name, jlfunc, jlrettype, ...) \
    func_name##_jlfunc = daggerc_register_func(jlfunc, jlrettype, __VA_ARGS__)

void dagger_init(char init_julia);
void dagger_shutdown(char shutdown_julia);

dagger_spec_t *dagger_create_spec(void *fn, dagger_type_t rettype, int nargs);
jl_value_t **dagger_alloc_task_args(dagger_spec_t *spec);

dagger_task_t *dagger_spawn(dagger_spec_t *spec, jl_value_t **args);
jl_value_t *dagger_fetch(dagger_task_t *task);
void dagger_wait(dagger_task_t *task);

#endif
