# Dagger's Supported Features

This is the list of features that Dagger currently supports, as well as their
status. If you find a bug in any of the "Great Support" features, please open
an issue - for "Moderate Support" or "Poor Support", pull requests are always
welcome!

## Core

| Feature      | Status       | Notes | Contact (JuliaLang Slack) |
| ------------- | ------------- | ----- | ------- |
| Multithreading | :heavy_check_mark: Great Support | | @jpsamaroo |
| Distributed | :warning: Moderate Support | Thread-safety issues [#101](https://github.com/JuliaLang/Distributed.jl/pull/101) | @jpsamaroo |
| Fault Tolerance | :x: Poor Support | Sometimes unreliable, may fail to recover in trivial cases | @jpsamaroo |
| Checkpointing | :warning: Moderate Support | Inconvenient API | @jpsamaroo |

## DArrays

| Feature      | Status       | Notes | Contact (JuliaLang Slack) |
| ------------- | ------------- | ----- | ------- |
| Broadcast | :heavy_check_mark: Great Support | | @jpsamaroo |
| Map/Reduce | :heavy_check_mark: Great Support | | @jpsamaroo |
| Indexing/Slicing | :x: Poor Support | Incorrect/broken slicing, poor performance | @jpsamaroo |
| Matmul | :warning: Moderate Support | Some incorrect boundschecks (for `syrk`) | @Rabab53 |
| Cholesky | :warning: Moderate Support | Missing repartitioning support | @Rabab53 |
| Sparse Arrays | :x: Poor Support | Needs more supported operations and tests | @jpsamaroo |

## GPUs

| Feature      | Status       | Notes | Contact (JuliaLang Slack) |
| ------------- | ------------- | ----- | ------- |
| NVIDIA GPUs | :heavy_check_mark: Great Support | | @jpsamaroo |
| AMD GPUs | :heavy_check_mark: Great Support | | @jpsamaroo |
| Apple GPUs | :warning: Moderate Support | Missing linalg operations | @jpsamaroo or @Rabab53 |
| Intel GPUs | :warning: Moderate Support | Missing linalg operations | @jpsamaroo or @Rabab53 |
| KernelAbstractions Integration | :warning: Moderate Support | Missing synchronization optimizations | @jpsamaroo |


# Dagger's Roadmap

This is the list of features and improvements that are planned for Dagger.
Please consider contributing an improvement if you feel able to do so!

## DArrays

| Feature      | Issue/PR #      | Help Wanted | Testers Wanted | Details       | Contact (JuliaLang Slack) |
| ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |
| LU Factorization | None | :warning: WIP, need pivoting impl. | N/A | LU factorization for DArray | @Rabab53 |
| QR Factorization | #529 | :warning: WIP | N/A | QR factorization for DArray | @fda-tome |
| Triangular Solve | None | :warning: WIP | N/A | Triangular solve (`A \ B`) and `ldiv` for DArray | @Rabab53 |
| SVD | None | :heavy_check_mark: | N/A | SVD for DArray | @fda-tome or @Rabab53 |

## Datadeps

| Feature      | Issue/PR #      | Help Wanted | Testers Wanted | Details       | Contact (JuliaLang Slack) |
| ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |
| Datadeps Stencil Helper | None | :heavy_check_mark: | N/A | Helper for stencil computations which lowers to Datadeps | @jpsamaroo |
| Datadeps MPI Support | None | :warning: WIP | :heavy_check_mark: | Datadeps support for automatic MPI usage | @fda-tome or @jpsamaroo |
| Datadeps Memory Awareness | None | :heavy_check_mark: | N/A | Datadeps support for memory awareness and OOM avoidance | @fda-tome or @jpsamaroo |

## GPUs

| Feature      | Issue/PR #      | Help Wanted | Testers Wanted | Details       | Contact (JuliaLang Slack) |
| ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |
| GPU Stream Scheduler | None | :heavy_check_mark: | N/A | Scheduler for assigning tasks to different streams on the same GPU device | @jpsamaroo |
| KernelAbstractions Backend | None | :heavy_check_mark: | N/A | KA DaggerBackend for SPMD programming | @jpsamaroo |
