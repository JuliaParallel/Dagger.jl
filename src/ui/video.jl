import .FFMPEG, .FileIO

function combine_gantt_images(svg_path::String, prof_path::String, delay)
    fr = 1 / delay
    svg_files = [joinpath(svg_path,file) for file in readdir(svg_path) if endswith(file, ".svg")]
    prof_files = [joinpath(prof_path,file) for file in readdir(prof_path) if endswith(file, ".svg")]
    foreach(file->run(`convert $file $(splitext(file)[1]*".png")`), svg_files)
    foreach(file->run(`convert $file $(splitext(file)[1]*".png")`), prof_files)
    foreach(rm, svg_files)
    foreach(rm, prof_files)
    FFMPEG.exe("-r", "$fr",
               "-f", "image2",
               "-s", "1920x1080",
               "-i", "$svg_path/%d.png",
               "-vcodec", "libx264",
               "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
               "-crf", "25",
               "-pix_fmt", "yuv420p",
               "$svg_path/final.mp4")
    FFMPEG.exe("-r", "$fr",
               "-f", "image2",
               "-s", "1920x1080",
               "-i", "$prof_path/%d.png",
               "-vcodec", "libx264",
               "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
               "-crf", "25",
               "-pix_fmt", "yuv420p",
               "$prof_path/final.mp4")
    return (gantt="$svg_path/final.mp4",
            profile="$prof_path/final.mp4")
end
