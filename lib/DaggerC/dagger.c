#include "dagger.h"

void dagger_init(char init_julia)
{
    // Initialize Julia
    printf("Initializing Julia\n");
    if (init_julia)
        jl_init();

    // Load DaggerC
    printf("Loading DaggerC\n");
    jl_eval_string("using DaggerC");
    if (jl_exception_occurred()) {
        jl_static_show(1, jl_exception_occurred());
        jl_printf(1, "\n");
        //jlbacktrace();
        printf("Failed to load DaggerC: %s\n", jl_typeof_str(jl_exception_occurred()));
        exit(EXIT_FAILURE);
    }
    jl_eval_string("println(DaggerC.Spec)");

    // Generate pointers to Dagger Julia functions
    printf("Generating cfunction pointers\n");
    //GENERATE_JLFUNC(dagger_spawn, "DaggerC.spawn", "Ptr{Cvoid}", 2, "Ptr{DaggerC.Spec}", "Ptr{Ptr{Cvoid}}");
    //GENERATE_JLFUNC(dagger_spawn, "DaggerC.spawn", "Ptr{Cvoid}", 2, "Ptr{Cvoid}", "Ptr{Ptr{Cvoid}}");
    dagger_spawn_jlfunc = jl_unbox_voidpointer(jl_eval_string("DaggerC.spawn_jlfunc[]"));
    printf("dagger_spawn_jlfunc: %llx\n", dagger_spawn_jlfunc);
    GENERATE_JLFUNC(dagger_fetch, "DaggerC.fetch", "Ptr{Cvoid}", 1, "Ptr{Cvoid}");
    GENERATE_JLFUNC(dagger_wait, "DaggerC.wait", "Nothing", 1, "Ptr{Cvoid}");
}

void dagger_shutdown(char shutdown_julia)
{
    if (shutdown_julia)
        jl_atexit_hook(0);
}

_Atomic(int) spec_id_counter = 0;

dagger_spec_t *dagger_create_spec(void *fn, dagger_type_t rettype, int nargs)
{
    dagger_spec_t *spec = (dagger_spec_t *)malloc(sizeof(dagger_spec_t));
    spec->id = atomic_fetch_add(&spec_id_counter, 1);
    spec->fn = fn;
    spec->rettype = rettype;
    if (nargs)
        spec->argtypes = (dagger_type_t *)malloc(sizeof(dagger_type_t) * nargs);
    else
        spec->argtypes = (dagger_type_t *)NULL;
    spec->nargs = nargs;
    return spec;
}

jl_value_t **dagger_alloc_task_args(dagger_spec_t *spec)
{
    return (jl_value_t **)malloc(sizeof(jl_value_t *) * spec->nargs);
}
dagger_task_t *dagger_spawn(dagger_spec_t *spec, jl_value_t **args)
{
    dagger_task_t *task = (dagger_task_t *)malloc(sizeof(dagger_task_t));
    task->value = dagger_spawn_jlfunc(spec, args);
    return task;
}

jl_value_t *dagger_fetch(dagger_task_t *task)
{
    return dagger_fetch_jlfunc(task->value);
}

void dagger_wait(dagger_task_t *task)
{
    dagger_wait_jlfunc(task->value);
}
