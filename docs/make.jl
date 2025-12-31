using WiNDCHousehold
using Documenter

DocMeta.setdocmeta!(WiNDCHousehold, :DocTestSetup, :(using WiNDCHousehold); recursive=true)


const _PAGES = [
    "Introduction" => [
        "index.md",
    #    "quick_start.md",
    #    "yaml.md",
    ],
    "Data" => [
    #    "data/maps.md",
        "data/input_data.md",
    #    "data/disaggregation.md",
    #    "data/calibration.md",
        "data/sets.md",
    ],
    "Model" => [
        "model/model.md"
    ],
    "API Reference" => ["api.md"],
]


#literate_files = Dict(
#    #"basic_rc" => ( 
#    #    input = "src/Tutorials/robinson_crusoe/basic_rc.jl",
#    #    output = "src/Tutorials/robinson_crusoe/"
#    #),
#
#)
#
#
#for (name, paths) in literate_files
#    EXAMPLE = joinpath(@__DIR__, paths.input)
#    OUTPUT = joinpath(@__DIR__, paths.output)
#    Literate.markdown(EXAMPLE, 
#                      OUTPUT;
#                      name = name)
#end



makedocs(;
    modules=[WiNDCHousehold],
    authors="Mitch Phillipson",
    sitename="WiNDCHousehold.jl",
    format=Documenter.HTML(;
        canonical="https://github.com/uw-windc/WiNDCHousehold.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=_PAGES
)

deploydocs(;
    repo = "github.com/uw-windc/WiNDCHousehold.jl",
    devbranch = "main",
    branch = "gh-pages",
    push_preview = true
)