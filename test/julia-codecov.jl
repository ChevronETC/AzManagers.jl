using Pkg

Pkg.activate("coveragetempenv", shared=true)

Pkg.add(PackageSpec(name="CoverageTools"))

using CoverageTools

# Usually you dont specify the path here but I had to because it kept saying file not found
# Also I'm pretty sure this is the wrong path, it's supposed to be a package src file
pf = process_folder("/home/cvx/.julia/packages/AzManagers/")

LCOV.writefile("lcov.info", pf)