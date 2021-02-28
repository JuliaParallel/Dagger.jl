import .FFMPEG, .FileIO

function combine_images(path, delay)
    fr = 1 / delay
    files = [joinpath(path,file) for file in readdir(path) if endswith(file, ".svg")]
    foreach(file->run(`convert $file $(splitext(file)[1]*".png")`), files)
    foreach(rm, files)
    FFMPEG.exe("-r", "$fr",
               "-f", "image2",
               "-s", "1920x1080",
               "-i", "$path/%d.png",
               "-vcodec", "libx264",
               "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
               "-crf", "25",
               "-pix_fmt", "yuv420p",
               "$path/final.mp4")
end

function combine_gantt_images(ctx, svg_path::String, prof_path::String, delay)
    combine_images(svg_path, delay)
    ctx.profile && combine_images(prof_path, delay)
    return (gantt="$svg_path/final.mp4",
            profile="$prof_path/final.mp4")
end
