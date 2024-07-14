package core

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sergi/go-diff/diffmatchpatch"
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

func getRemovedStatementsAndUpdateDb(ctx *Context) []string {
  var removed []string

  for _, f := range getListOfStatementsInDb(ctx) {
    nameFound := false
    hashFound := false
    for _, s := range *ctx.Stmts {
      if s.Hash == *f.stmtHash {
        hashFound = true
      }

      if s.HasName && f.stmtName != nil && s.Name == *f.stmtName {
        nameFound = true
      }
    }

    if !hashFound && !nameFound {
      removed = append(removed, *f.stmt)
      removeStmtByHash(ctx, *f.stmtHash)
    }
  }

  return removed
}

func generateDiffComment(ctx *Context, stmt *ParsedStmt) string {
  prevDeparsed, e := deparseRawStmt(stmt.PrevStmt)
  perr(e)

  diffText := ""

  dmp := diffmatchpatch.New()
  cdiff := dmp.DiffMain(prevDeparsed, stmt.Deparsed, false)

  for _, d := range cdiff {
    switch d.Type {
      case diffmatchpatch.DiffInsert: {
        diffText += fmt.Sprintf("+++ %s\n", d.Text)
      }

      case diffmatchpatch.DiffDelete: {
        diffText += fmt.Sprintf("--- %s\n", d.Text)
      }

      case diffmatchpatch.DiffEqual: {
        diffText += fmt.Sprintf("@@@ %s\n", d.Text)
      }
    }
  }

  return fmt.Sprintf(`/*
%s
---------- CURRENT VERSION ----------
%s
----------   CHANGED TO    ----------
%s
----------   CHANGE DIFF   ----------
%s
*/`, VALIDATE_MIGRATIONS_STRING, prevDeparsed, stmt.Deparsed, diffText)

}

func generateRemovedComment(ctx *Context, remove string) string {
  return fmt.Sprintf(`/*
%s
-----------     REMOVED    ----------
%s
*/`, VALIDATE_MIGRATIONS_STRING, remove);
}

func writeMigrationsToNextMigration(ctx *Context) int {
  nextMigrationFile := filepath.Join(ctx.MigrationPath, getNextMigrationFileName(ctx))

  var migrations []string

  for _, stmt := range *ctx.Stmts {
    switch stmt.Status {
      case NEW: {
        migrations = append(migrations, stmt.Deparsed) 
        addStmtToDb(ctx, stmt)
      }

      case CHANGED: {
        migrations = append(migrations, generateDiffComment(ctx, stmt))
        updateStmtInDb(ctx, stmt)
      }
    }
  }

  for _, removed := range getRemovedStatementsAndUpdateDb(ctx) {
    migrations = append(migrations, generateRemovedComment(ctx, removed))
  }

  f, err := os.Create(nextMigrationFile)
  perr(err)

  defer f.Close()

  f.WriteString(string(strings.Join(migrations, "\n")))

  return len(migrations)
}

func executeMigration(ctx *Context, migrationFile string)  {
  filename := extractFileFromPath(migrationFile)
  code := readFileToString(ctx, migrationFile)

  _, err := ctx.DbTx.Exec(code)
  perr(err)

  _, err = ctx.DbTx.Exec("insert into schemaflow.migrations (file_name, file_hash) values ($1, $2)", filename, HashFile(migrationFile))
  perr(err)
}

func checkForUnresolvedMigrations(ctx *Context) {
  unresolved_migration_files := getMigrationFilesWithUnresolvedMigrations(ctx)

  if len(unresolved_migration_files) > 0 {
    log.Fatalf("The following files have unresolved migrations: %s\n", strings.Join(unresolved_migration_files, ", "))
  }
}

func areMigrationsRequired(ctx *Context) bool {
  for _, stmt := range *ctx.Stmts {
    if stmt.Status == UNKNOWN {
      log.Fatalf("Status of statement %v is UNKNOWN. This is a bug.", stmt)
    } else if stmt.Status != UNCHANGED {
      return true
    }
  }

  return false
}

func runMigrations(ctx *Context) {
  migrations := getListOfUnexecutedMigrations(ctx)

  for _, migration := range migrations {
    log.Printf("Executing %s\n", migration)
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
  
  if !areMigrationsRequired(ctx) {
    log.Println("No migrations required.")
    return
  }

  next_migration := getNextMigrationFileName(ctx)

  numOfMigrationsRun := writeMigrationsToNextMigration(ctx)

  log.Printf("%d migrations have been written to %s\n", numOfMigrationsRun, next_migration)
}

func Migrate(ctx *Context) {
  setup(ctx)

  if len(getListOfUnexecutedMigrations(ctx)) == 0 {
    log.Println("All migrations have already been executed.")
    return
  }

  runMigrations(ctx)
}

func Clean(ctx *Context) {

}
