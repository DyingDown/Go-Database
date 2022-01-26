/*
 *	Data Manager
 *	Manage Pager and Index
 *	DM receives a condition and return data by calling Pager and Index
 *	DM Revie
 */
package dm

import (
	"fmt"
	"go-database/parser/ast"
	"go-database/storage/pager"
	"go-database/storage/pager/pagedata"
)

type DataManager struct {
	pager *pager.Pager
}

func CreateDM(path string) *DataManager {
	p := pager.CreateFile(path)
	return &DataManager{
		pager: p,
	}
}

func OpenDM(path string) *DataManager {
	p := pager.OpenFile(path)
	return &DataManager{
		pager: p,
	}
}

// @description: selet rows according to sql where statement
func (dm *DataManager) SelectData(st ast.SQLSelectStatement) (<-chan ast.Row, error) {
	rows := make(chan ast.Row, 100)
	// get table info
	tableInfo := dm.pager.GetMetaData().GetTableInfo(st.Table)
	if tableInfo == nil {
		close(rows)
		return nil, fmt.Errorf("can't get table %s, it may not exist", st.Table)
	}
	// if there is where conditions
	if st.Expr.IsWhereExists() {
		// there may be many conditions
		for i := range st.Expr.Exprs {
			expr := st.Expr.Exprs[i]
			if expr.IsEqual() {
				dm.eqSearch(rows, expr, tableInfo)
			} else if expr.NotEqual() {
				dm.nEqSearch(rows, expr, tableInfo)
			} else {
				dm.rangeSearch(rows, expr, tableInfo)
			}
		}
	} else {
		// if no conditions, return the whole table
		dm.scanTable(rows, tableInfo)
	}
	return rows, nil
}

func (dm *DataManager) eqSearch(rows chan<- ast.Row, expr ast.SQLSingleExpression, tableInfo *pagedata.TableInfo) {
	if expr.LeftVal.GetType() == ast.COLUMN {
		// get column name
		columnName := expr.LeftVal.GetString()
		if columnName == "" {
			// TODO: 应该由最后一个expr来关闭chan
			close(rows)
			return
		}
		//
	} else {
		if expr.LeftVal == expr.RightVal {
			// if the condition satisfies every row
			dm.scanTable(rows, tableInfo)
		} else {
			// if conditions dissatisfies each row

			// TODO: 应该由最后一个expr来关闭chan
			close(rows)
			return
		}
	}
}

func (dm *DataManager) nEqSearch(rows chan<- ast.Row, expr ast.SQLSingleExpression, tableInfo *pagedata.TableInfo) {

}

func (dm *DataManager) rangeSearch(rows chan<- ast.Row, expr ast.SQLSingleExpression, tableInfo *pagedata.TableInfo) {

}

func (dm *DataManager) scanTable(rows chan<- ast.Row, tableInfo *pagedata.TableInfo) {

}
