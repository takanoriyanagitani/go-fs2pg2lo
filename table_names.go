package fs2pg2lo

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrInvalidTableName        error = errors.New("invalid table name")
	ErrFileSizeCategoryUnknown error = errors.New("file size category unknown")
)

type TableName struct{ checked string }

type UncheckedTableName struct{ unchecked string }

func (u UncheckedTableName) ToError() error {
	return fmt.Errorf("%w: %s", ErrInvalidTableName, u.unchecked)
}

type TableNameChecker func(
	context.Context, UncheckedTableName,
) (TableName, error)

type IsTableExists func(context.Context, UncheckedTableName) bool

func (i IsTableExists) ToChecker() TableNameChecker {
	var empty TableName

	return func(ctx context.Context, uchk UncheckedTableName) (TableName, error) {
		var found bool = i(ctx, uchk)
		switch found {
		case true:
			return TableName{checked: uchk.unchecked}, nil
		default:
			return empty, uchk.ToError()
		}
	}
}
