package models

import "gorm.io/gorm"

// MigrateStageIndex replaces the legacy global unique index on stage_id with a
// per-workflow composite (workflow_id, stage_id).
func MigrateStageIndex(db *gorm.DB) error {
	if err := db.Exec(`DROP INDEX IF EXISTS idx_stages_stage_id`).Error; err != nil {
		return err
	}
	return db.Exec(
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_stage ON stages (workflow_id, stage_id)`,
	).Error
}
