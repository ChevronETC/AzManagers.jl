using AzManagers, Distributed, Documenter

makedocs(sitename="AzManagers", modules=[AzManagers], warnonly=[:missing_docs])

deploydocs(
    repo = "github.com/ChevronETC/AzManagers.jl.git",
)