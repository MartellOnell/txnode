// Package txnode provides a transaction node for chaining SQL operations within a single transaction.
package txnode

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
)

// TxNode represents a node in a transaction chain.
// It manages the lifecycle of a SQL transaction across multiple operations.
type TxNode struct {
	isStart bool
	tx      *sql.Tx
	isEnd   bool
}

var (
	ErrTransactionArgsMismatch = errors.New("transaction args mismatch")
)

// New creates a new TxNode ready to start a transaction.
func New() *TxNode {
	return &TxNode{
		isStart: true,
	}
}

// UnsetEnd marks this node as not being the end of the transaction chain.
func (txn *TxNode) UnsetEnd() {
	txn.isEnd = false
}

// SetEnd marks this node as the end of the transaction chain.
func (txn *TxNode) SetEnd() {
	txn.isEnd = true
}

// PrepareQuery prepares a SQL statement. It begins a transaction on first call
// or reuses the existing transaction. Returns nil if txn is nil (non-transactional mode).
func (txn *TxNode) PrepareQuery(
	ctx context.Context,
	db *sql.DB,
	query string,
) (*sql.Stmt, error) {
	if txn == nil {
		stmt, err := db.PrepareContext(ctx, query)
		return stmt, err
	}

	if txn.isStart {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}

		txn.isStart = false
		txn.tx = tx

		stmt, err := tx.PrepareContext(ctx, query)
		return stmt, err
	}

	if txn.tx != nil {
		stmt, err := txn.tx.PrepareContext(ctx, query)
		return stmt, err
	}

	return nil, ErrTransactionArgsMismatch
}

// RollbackTransaction rolls back the transaction if one exists.
func (txn *TxNode) RollbackTransaction() error {
	if txn == nil || txn.tx == nil {
		return nil
	}

	return txn.tx.Rollback()
}

// CommitIfNeeded commits the transaction only if this node is marked as the end.
func (txn *TxNode) CommitIfNeeded() error {
	if txn == nil || txn.tx == nil || !txn.isEnd {
		return nil
	}

	return txn.tx.Commit()
}

// RollbackTransactionAndLog rolls back the transaction and logs both the rollback
// and the original error. Returns a wrapped error with the operation name.
func (txn *TxNode) RollbackTransactionAndLog(
	log *slog.Logger,
	op string,
	err error,
) error {
	rollbackErr := txn.RollbackTransaction()
	if rollbackErr != nil {
		log.Error(fmt.Sprintf("%s: rollback transaction: %v", op, rollbackErr))
	}
	log.Error(fmt.Sprintf("%s: %v", op, err))
	return fmt.Errorf("%s: %w", op, err)
}
