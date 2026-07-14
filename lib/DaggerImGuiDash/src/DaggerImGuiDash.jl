module DaggerImGuiDash

using Printf

include("logging.jl")
include("ui.jl")

using CImGui
import CSyntax: @c
import CSyntax.CStatic: @cstatic
import CImGui: ImGuiGLFWBackend
if CImGui.opengl_major_version == 2
import CImGui: ImGuiOpenGL2Backend
const OpenGLBackend = ImGuiOpenGL2Backend
elseif CImGui.opengl_major_version >= 3
import CImGui: ImGuiOpenGLBackend
const OpenGLBackend = ImGuiOpenGLBackend
end
using ModernGL
using GLFW
#using ImPlot

function run_gui(r::Renderer)

CLEAR_COLOR = Cfloat[0.45, 0.55, 0.60, 1.00]

glsl_version = 130

# setup GLFW error callback
#? error_callback(err::GLFW.GLFWError) = @error "GLFW ERROR: code $(err.code) msg: $(err.description)"
#? GLFW.SetErrorCallback(error_callback)

# create window
window = GLFW.Window(;name="DaggerImGuiDash", windowhints=[], contexthints=[
    (GLFW.CONTEXT_VERSION_MAJOR, 2),
    (GLFW.CONTEXT_VERSION_MINOR, 0),
])
GLFW.MakeContextCurrent(window)
GLFW.SwapInterval(1)  # enable vsync

# setup Dear ImGui context
ctx = CImGui.CreateContext()

# setup Dear ImGui style
CImGui.StyleColorsDark()

# load Fonts
fonts = unsafe_load(CImGui.GetIO().Fonts)
CImGui.AddFontDefault(fonts)

# setup Platform/Renderer bindings
glfw_ctx = ImGuiGLFWBackend.create_context(window.handle, install_callbacks = true)
ImGuiGLFWBackend.init(glfw_ctx)
opengl_ctx = OpenGLBackend.create_context(glsl_version)
OpenGLBackend.init(opengl_ctx)

try
    while GLFW.WindowShouldClose(window) == 0
        GLFW.PollEvents()
        # start the Dear ImGui frame
        OpenGLBackend.new_frame(opengl_ctx) #ImGui_ImplOpenGL3_NewFrame()
        ImGuiGLFWBackend.new_frame(glfw_ctx) #ImGui_ImplGlfw_NewFrame()
        CImGui.NewFrame()

        if CImGui.Begin("DaggerImGuiDash")
            inner_gui(r)
            CImGui.End()
        end

        # rendering
        CImGui.Render()
        GLFW.MakeContextCurrent(window)

        display_w, display_h = GLFW.GetFramebufferSize(window)

        glViewport(0, 0, display_w, display_h)
        glClearColor(CLEAR_COLOR...)
        glClear(GL_COLOR_BUFFER_BIT)
        OpenGLBackend.render(opengl_ctx) #ImGui_ImplOpenGL3_RenderDrawData(CImGui.GetDrawData())

        GLFW.MakeContextCurrent(window)
        GLFW.SwapBuffers(window)

        # Let background tasks run
        yield()
    end
catch e
    @error "Error in renderloop!" exception=e
    Base.show_backtrace(stderr, catch_backtrace())
finally
    OpenGLBackend.shutdown(opengl_ctx) #ImGui_ImplOpenGL3_Shutdown()
    ImGuiGLFWBackend.shutdown(glfw_ctx) #ImGui_ImplGlfw_Shutdown()
    CImGui.DestroyContext(ctx)
    GLFW.DestroyWindow(window)
end

end # function run_gui()

end # module DaggerImGuiDash
