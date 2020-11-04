# using Pkg
# Pkg.activate("coveragetempenv", shared=true)
# Pkg.add(PackageSpec(name="CoverageTools"))

# using CoverageTools

# # Usually you dont specify the path here but I had to because it kept saying file not found
# # Also I'm pretty sure this is the wrong path, it's supposed to be a package src file
# pf = process_folder("/home/cvx/.julia/packages/AzManagers/")
# LCOV.writefile("lcov.info", pf)


using Coverage
# process '*.cov' files
coverage = process_folder("/home/cvx/.julia/packages/AzManagers/") # defaults to src/; alternatively, supply the folder name as argument
# coverage = append!(coverage, process_folder("deps"))
# # process '*.info' files
# coverage = merge_coverage_counts(coverage, filter!(
#     let prefixes = (joinpath(pwd(), "src", ""),
#                     joinpath(pwd(), "deps", ""))
#         c -> any(p -> startswith(c.filename, p), prefixes)
#     end,
#     LCOV.readfolder("test")))
# # Get total coverage for all Julia files
# covered_lines, total_lines = get_summary(coverage)
# # Or process a single file
# @show get_summary(process_file(joinpath("src", "MyPkg.jl")))

LCOV.writefile("lcov.info", coverage)
