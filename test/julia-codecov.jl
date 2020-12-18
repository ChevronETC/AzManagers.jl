using Coverage, AzManagers
coverage = process_folder(pkgdir(AzManagers))
LCOV.writefile("lcov.info", coverage)
