package core

import (
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strings"
)

func getMigrationFilesSorted(ctx *Context) []string {
  files := ListAllFilesInPath(ctx.MigrationPath)
  sort.Strings(files)
  return files
}

func getListOfUnexecutedMigrations(ctx *Context) []string {
  var unexecutedMigrations []string

  executedFiles := getListOfExecutedMigrationFiles(ctx)

  for _, mf := range getMigrationFilesSorted(ctx) {
    shouldBeExecuted := true
    for _, ef := range executedFiles {
      if extractFileFromPath(mf) == extractFileFromPath(ef.fileName) {
        shouldBeExecuted = false
        break
      }
    }

    if shouldBeExecuted {
      unexecutedMigrations = append(unexecutedMigrations, mf)
    }
  }

  return unexecutedMigrations
}

func getNextMigrationFileName(ctx *Context) string {
  all_migrations := getMigrationFilesSorted(ctx)
  migration_number := len(all_migrations)
  return fmt.Sprintf("%04d.sql", migration_number)
}

func isValidationStringInFile(ctx *Context, file string) bool {
  for _, line := range strings.Split(readFileToString(ctx, file), "\n") {
    if line == VALIDATE_MIGRATIONS_STRING {
      return true
    }
  }

  return false
}

func getMigrationFilesWithUnresolvedMigrations(ctx *Context) []string {
  var migration_files []string

  for _, file := range getMigrationFilesSorted(ctx) {
    if isValidationStringInFile(ctx, file) {
      migration_files = append(migration_files, file)
    }
  }

  return migration_files
}

func getTamperedMigrations(ctx *Context) []string {
  var tampered []string
  
  for _, em := range getListOfExecutedMigrationFiles(ctx) {
    path := filepath.Join(ctx.MigrationPath, em.fileName)

    if HashFile(path) != em.fileHash {
      tampered = append(tampered, path)
    }
  }

  return tampered
}

func checkExecutedMigrationsUnchanged(ctx *Context) {
  tampered := getTamperedMigrations(ctx)
  if len(tampered) > 0 {
    log.Fatalf("The following executed migrations have been tampered with: %s\nCannot continue.\n", strings.Join(tampered, ", "))
  }
}

func writeMigrationsToNextMigration(ctx *Context) {

}

func executeMigration(ctx *Context, migrationFile string)  {
  filename := extractFileFromPath(migrationFile)
  code := readFileToString(ctx, migrationFile)

  _, err := ctx.DbTx.Exec(code)
  perr(err)

  _, err = ctx.DbTx.Exec("insert into morph.migrations (file_name, file_hash) values ($1, $2)", filename, HashFile(migrationFile))
  perr(err)
}

func checkForUnresolvedMigrations(ctx *Context) {
  unresolved_migration_files := getMigrationFilesWithUnresolvedMigrations(ctx)

  if len(unresolved_migration_files) > 0 {
    log.Fatalf("The following files have unresolved migrations: %s\n", strings.Join(unresolved_migration_files, ", "))
  }
}

func runMigrations(ctx *Context) {
  migrations := getListOfUnexecutedMigrations(ctx)

  for _, migration := range migrations {
    executeMigration(ctx, migration)
  }
}

func setup(ctx *Context) {
  checkForUnresolvedMigrations(ctx)
  checkExecutedMigrationsUnchanged(ctx)
}

func MakeMigrations(ctx *Context) {
  setup(ctx)

  ctx.Stmts = buildParsedStmts(ctx)
}

func Migrate(ctx *Context) {
  setup(ctx)

  runMigrations(ctx)
}

func Clean(ctx *Context) {

}
