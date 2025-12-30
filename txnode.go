package txnode

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
)

type TxNode struct {
	isStart bool
	tx      *sql.Tx
	isEnd   bool
}

var (
	ErrTransactionArgsMismatch = errors.New("transaction args mismatch")
)

func New() *TxNode {
	return &TxNode{
		isStart: true,
	}
}

func (txn *TxNode) UnsetEnd() {
	txn.isEnd = false
}

func (txn *TxNode) SetEnd() {
	txn.isEnd = true
}

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

func (txn *TxNode) RollbackTransaction() error {
	if txn == nil || txn.tx == nil {
		return nil
	}

	return txn.tx.Rollback()
}

func (txn *TxNode) CommitIfNeeded() error {
	if txn == nil || txn.tx == nil || !txn.isEnd {
		return nil
	}

	return txn.tx.Commit()
}

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
