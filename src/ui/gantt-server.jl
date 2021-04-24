using .Mux

function serve_gantt(svg_path, prof_path; port=8000)
    # Setup Mux app
    @app gantt_app = (
        Mux.defaults,
        page(req->begin
            data = String(read(svg_path))
            prof = String(read(prof_path))
            html = """
            <html>
            <head>
            <title>
            Dagger: Scheduler Gantt Chart
            </title>
            <meta http-equiv="refresh" content="$delay">
            </head>
            <body>
            $data
            <br/>
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
    Threads.@spawn serve(gantt_app, port)
end
