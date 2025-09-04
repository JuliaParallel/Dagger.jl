function gmres(A::DArray, b::DVector; x0=nothing, m=length(b), tol=1e-6, maxiter=100)
    """
    GMRES algorithm for solving Ax = b
    
    Args:
        A: coefficient matrix (or function that computes A*v)
        b: right-hand side vector
        x0: initial guess (default: zero vector)
        m: restart parameter (default: no restart)
        tol: convergence tolerance
        maxiter: maximum number of restarts
    
    Returns:
        x: solution vector
        residual_norm: final residual norm
        iterations: number of iterations
    """
    n = length(b)
    x = x0 === nothing ? zeros(AutoBlocks(), n) : DArray(copy(x0))
    
    # Initial residual
    r = b - A * x
    β = norm(r)
    
    if β < tol
        return x, β, 0
    end
    
    for restart in 1:maxiter
        # Krylov subspace basis vectors
        V = zeros(AutoBlocks(), n, m + 1)
        V[:, 1] = r / β
        
        # Upper Hessenberg matrix
        H = zeros(m + 1, m)
        
        # Givens rotation matrices (store cos and sin)
        cs = zeros(m)
        sn = zeros(m)
        
        # RHS for least squares problem
        e1 = zeros(AutoBlocks(), m + 1)
        e1[1] = β
        
        # Arnoldi iteration
        for j in 1:m
            # Apply matrix to current basis vector
            w = A * V[:, j]
            
            # Modified Gram-Schmidt orthogonalization
            for i in 1:j
                H[i, j] = dot(w, V[:, i])
                w -= H[i, j] * V[:, i]
            end
            
            H[j + 1, j] = norm(w)
            
            # Check for breakdown
            if abs(H[j + 1, j]) < eps()
                m = j
                break
            end
            
            V[:, j + 1] = w / H[j + 1, j]
            
            # Apply previous Givens rotations to new column of H
            for i in 1:(j-1)
                temp = cs[i] * H[i, j] + sn[i] * H[i + 1, j]
                H[i + 1, j] = -sn[i] * H[i, j] + cs[i] * H[i + 1, j]
                H[i, j] = temp
            end
            
            # Compute new Givens rotation
            if abs(H[j + 1, j]) < eps()
                cs[j] = 1.0
                sn[j] = 0.0
            else
                if abs(H[j + 1, j]) > abs(H[j, j])
                    τ = H[j, j] / H[j + 1, j]
                    sn[j] = 1.0 / sqrt(1 + τ^2)
                    cs[j] = sn[j] * τ
                else
                    τ = H[j + 1, j] / H[j, j]
                    cs[j] = 1.0 / sqrt(1 + τ^2)
                    sn[j] = cs[j] * τ
                end
            end
            
            # Apply new Givens rotation
            temp = cs[j] * H[j, j] + sn[j] * H[j + 1, j]
            H[j + 1, j] = -sn[j] * H[j, j] + cs[j] * H[j + 1, j]
            H[j, j] = temp
            
            # Apply rotation to RHS
            temp = cs[j] * e1[j] + sn[j] * e1[j + 1]
            e1[j + 1] = -sn[j] * e1[j] + cs[j] * e1[j + 1]
            e1[j] = temp
            
            # Check convergence
            residual_norm = abs(e1[j + 1])
            if residual_norm < tol
                m = j
                break
            end
        end
        
        # Solve upper triangular system H[1:m, 1:m] * y = e1[1:m]
        y = zeros(m)
        for i in m:-1:1
            y[i] = e1[i]
            for k in (i+1):m
                y[i] -= H[i, k] * y[k]
            end
            y[i] /= H[i, i]
        end
        
        # Update solution
        for i in 1:m
            x += y[i] * V[:, i]
        end
        
        # Check final convergence
        r = b - A * x
        β = norm(r)
        
        if β < tol
            return x, β, restart
        end
    end
    
    return x, β, maxiter
end

# Example usage
function example_usage()
    # Create test problem
    n = 100
    A = DArray(randn(n, n) + 5*I)  # Well-conditioned matrix
    x_true = randn(AutoBlocks(), n)
    b = A * x_true
    
    # Solve with GMRES
    allowscalar(false) do
        x_gmres, res_norm, iters = gmres(A, b, tol=1e-10)
    end
    
    println("GMRES converged in $iters iterations")
    println("Final residual norm: $res_norm")
    println("Solution error: $(norm(x_gmres - x_true))")
    
    return x_gmres
end
