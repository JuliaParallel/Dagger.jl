#include "dagger.h"

JULIA_DEFINE_FAST_TLS

int64_t myfunc(int64_t arg1, int64_t arg2)
{
    return arg1 + arg2;
}

int main(int argc, char *argv[])
{
    dagger_init(1);

    // Register our task spec
    dagger_spec_t *myfunc_spec = dagger_create_spec(myfunc,
                                                    dagger_type_int64,
                                                    2);
    myfunc_spec->argtypes[0] = dagger_type_int64;
    myfunc_spec->argtypes[1] = dagger_type_int64;

    // Spawn some tasks
    jl_value_t **args1 = dagger_alloc_task_args(myfunc_spec);
    args1[0] = jl_box_int64(1);
    args1[1] = jl_box_int64(2);
    dagger_task_t *task1 = dagger_spawn(myfunc_spec, args1);

    jl_value_t **args2 = dagger_alloc_task_args(myfunc_spec);
    args1[0] = jl_box_int64(3);
    args1[1] = jl_box_int64(4);
    dagger_task_t *task2 = dagger_spawn(myfunc_spec, args1);

    jl_value_t **args3 = dagger_alloc_task_args(myfunc_spec);
    args3[0] = task1->value;
    args3[1] = task2->value;
    dagger_task_t *task3 = dagger_spawn(myfunc_spec, args3);

    // Fetch the task's result
    jl_value_t *result = dagger_fetch(task3);
    printf("Got result: %d\n", jl_unbox_int64(result));

    dagger_shutdown(1);

    return 0;
}
