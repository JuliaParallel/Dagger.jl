module DaggerMuxExt

@static if isdefined(Base, :get_extension)
    using Mux
else
    using .Mux
end

using Dagger

function serve_gantt(svg_path, prof_path; port=8000, delay=5)
    # Setup Mux app
    @app gantt_app = (
        Mux.defaults,
        page(req->begin
            data = String(read(svg_path))
            html = """
            <html>
            <head>
            <title>
            Dagger: Scheduler Gantt Chart
            </title>
            <meta http-equiv="refresh" content="$delay">
            </head>
            <body>
            <a href="/profile">Go to profile data</a>
            $data
            </body>
            </html>
            """
            hdrs = Dict(
                Symbol("Content-Type")=>"text/html",
                Symbol("Cache-Control")=>"no-store",
            )
            Dict(
                :body=>html,
                :headers=>hdrs,
            )
        end),
        page("/profile", req->begin
            prof = String(read(prof_path))
            html = """
            <html>
            <head>
            <title>
            Dagger: Scheduler Profile Flamegraph
            </title>
            <meta http-equiv="refresh" content="$delay">
            </head>
            <body>
            $prof
            </body>
            </html>
            """
            hdrs = Dict(
                Symbol("Content-Type")=>"text/html",
                Symbol("Cache-Control")=>"no-store",
            )
            Dict(
                :body=>html,
                :headers=>hdrs,
            )
        end),
        Mux.notfound()
    )

    # Start serving app
    Threads.@spawn begin
        try
            serve(gantt_app, port)
        catch err
            @error exception=(err,catch_backtrace())
        end
    end
end

end # module DaggerMuxExt
