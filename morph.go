package main

import (
	"log"
	"morph/core"
)

func perr(err error) {
  core.Perr(err)
}

func main() {
  /* 
    TODO:
    - Bring back the versioned migrations system
    - Bring back the "makemigrations", "migrate", and "clean" commands"
    - Move the automatic migration generation files to their own module
    - Move the versioned migrations code to its own folder
    - By default, when generating the migration file, the migrations file should show that there is a change that was made to a specific statement, but not generate any migrations automatically.
    - If the "--auto-gen" flag is set, then for each changed statement migrations will attempted to be created automatically, but will still require validation.
  */

  ctx := core.ParseArgs()
  ctx.Db = core.CreateDbConnections(ctx.DbContext)

  defer ctx.Db.Close()

  db_tx, te := ctx.Db.Begin()
  ctx.DbTx = db_tx
  perr(te)

  core.Initialize(ctx) 

  switch ctx.Action {
    case core.MIGRATE: {
      core.Migrate(ctx)
    }

    case core.MAKEMIGRATIONS: {
      core.MakeMigrations(ctx)
    }

    case core.CLEAN: {
      core.Clean(ctx)
    }
  }

  perr(ctx.DbTx.Commit())

  log.Println("Done.")
}
