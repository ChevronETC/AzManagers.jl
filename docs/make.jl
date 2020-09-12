using AzManagers, Distributed, Documenter

makedocs(sitename="AzManagers", modules=[AzManagers])

deploydocs(
    repo = "github.com/ChevronETC/AzManagers.jl.git",
)