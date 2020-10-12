using Coverage
coverage = process_folder()
LCOV.writefile("lcov.info", coverage)
