/*
 *	Data Manager
 *	Manage Pager and Index
 *	DM receives a condition and returns data by calling Pager and Index
 *	DM Revie
 */
package dm

import (
	"fmt"
	"go-database/parser/ast"
	"go-database/parser/token"
	"go-database/storage/index"
	"go-database/storage/pager"
	"go-database/storage/pager/pagedata"
	"go-database/util"
	"sync"

	log "github.com/sirupsen/logrus"
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
		// TODO: there may be many conditions
		// for i := range st.Expr.Exprs {
		for i := 0; i < 1; i++ {
			expr := st.Expr.Exprs[i]
			if expr.NotEqual() {
				dm.notEqualSearch(rows, expr, tableInfo)
			} else {
				dm.simpleSearch(rows, expr, tableInfo)
			}
		}
	} else {
		// if no conditions, return the whole table
		dm.scanTable(rows, tableInfo, ast.SQLSingleExpression{})
	}
	return rows, nil
}

func (dm *DataManager) simpleSearch(rows chan<- ast.Row, expr ast.SQLSingleExpression, tableInfo *pagedata.TableInfo) {
	if expr.LeftVal.GetType() == ast.ST_COLUMN {
		// get column name
		columnName := expr.LeftVal.GetString()
		if columnName == "" {
			// TODO: 应该由最后一个expr来关闭chan
			close(rows)
			return
		}
		// more info about this column
		_, columnDefine := tableInfo.GetColumnInfo(columnName)
		if columnDefine == nil {
			// close
			close(rows)
			log.Errorf("column: %s does not exist", columnName)
			return
		}
		index := columnDefine.Index
		// if there is no index, scan all pages
		if index == nil {
			dm.scanTable(rows, tableInfo, expr)
			return
		}
		// if the target column is primary key column
		if tableInfo.GetPrimaryKey() == columnName {
			dm.pkEqSearch(rows, index, expr)
		} else {
			// if the column is not primary key
			_, pkColumn := tableInfo.GetColumnInfo(tableInfo.GetPrimaryKey())
			if pkColumn == nil {
				close(rows)
				log.Errorf("Table has no primary column: %s", tableInfo.GetPrimaryKey())
				return
			}
			// if the primary column has index
			pIndex := pkColumn.Index
			if pIndex == nil {
				close(rows)
				log.Errorf("Primary Index: %s not exist", tableInfo.GetPrimaryKey())
			}
			dm.nPkEqSearch(rows, index, pIndex, expr)
			return
		}
	} else {
		if expr.LeftVal == expr.RightVal {
			// if the condition satisfies every row
			dm.scanTable(rows, tableInfo, expr)
		} else {
			// if conditions dissatisfies each row
			// TODO: 应该由最后一个expr来关闭chan
			close(rows)
			return
		}
	}
}

// @description: not equal search
func (dm *DataManager) notEqualSearch(rows chan<- ast.Row, expr ast.SQLSingleExpression, tableInfo *pagedata.TableInfo) {
	dm.scanTable(rows, tableInfo, expr)
}

// @description: scan the whole table
// Start from the first page and ends with the last page
// For each page, scan all rows to find satisfied rows
func (dm *DataManager) scanTable(rows chan<- ast.Row, tableInfo *pagedata.TableInfo, expr ast.SQLSingleExpression) {
	// find which rows to compare
	whereFunc, err := dm.GetRowFilter(tableInfo, expr)
	if err != nil {
		close(rows)
		return
	}
	// scan all pages
	for i := tableInfo.FirstPage; i != 0; {
		page, err := dm.pager.GetPage(i, pagedata.NewRecordData())
		if err == nil {
			close(rows)
			return
		}
		// scan rows in one page
		dataRows := page.GetPageData().(*pagedata.RecordData).Rows() // get all rows
		for _, j := range dataRows {
			if whereFunc(j) {
				rows <- j
			}
		}
		i = page.GetNextPageNo()
	}
	close(rows)
}

// @description:  receives a sql expression and returns a function
// @param: sql expression
// @return: a function that can judge a row is satisfied or not
func (dm *DataManager) GetRowFilter(tableInfo *pagedata.TableInfo, expr ast.SQLSingleExpression) (func(row ast.Row) bool, error) {
	// if the both sides are columns
	if expr.LeftVal.GetType() == ast.ST_COLUMN && expr.RightVal.GetType() == ast.ST_COLUMN {
		leftColumnName := expr.LeftVal.GetString()
		rightColumName := expr.RightVal.GetString()
		if leftColumnName == "" || rightColumName == "" {
			return nil, fmt.Errorf("column name is empty")
		} else {
			leftIndex, leftColumn := tableInfo.GetColumnInfo(leftColumnName)
			rightIndex, rightColum := tableInfo.GetColumnInfo(rightColumName)
			if leftColumn == nil {
				return nil, fmt.Errorf("column: %s does not exist", leftColumnName)
			}
			if rightColum == nil {
				return nil, fmt.Errorf("column: %s does not exist", rightColumName)
			}
			if expr.CompareOp == token.EQUAL {
				return func(row ast.Row) bool {
					return row[leftIndex] == row[rightIndex]
				}, nil
			} else if expr.CompareOp == token.NOT_EQUAL {
				return func(row ast.Row) bool {
					return row[leftIndex] != row[rightIndex]
				}, nil
			} else {
				return nil, fmt.Errorf("compare operator: %v is not supported", expr.CompareOp)
			}
		}
	} else if expr.RightVal.GetType() == ast.ST_COLUMN {
		columnName := expr.RightVal.GetString()
		if columnName == "" {
			return nil, fmt.Errorf("column: %s does not exist", columnName)
		}
		i, columnDefine := tableInfo.GetColumnInfo(columnName)
		if columnDefine == nil {
			return nil, fmt.Errorf("column: %s does not exist", columnName)
		}
		if expr.CompareOp == token.EQUAL {
			return func(row ast.Row) bool {
				return row[i] == expr.LeftVal
			}, nil
		} else if expr.CompareOp == token.NOT_EQUAL {
			return func(row ast.Row) bool {
				return row[i] != expr.LeftVal
			}, nil
		} else {
			return nil, fmt.Errorf("compare operator: %v is not supported", expr.CompareOp)
		}
	} else if expr.LeftVal.GetType() == ast.ST_COLUMN {
		columnName := expr.LeftVal.GetString()
		if columnName == "" {
			return nil, fmt.Errorf("column: %s does not exist", columnName)
		}
		i, columnDefine := tableInfo.GetColumnInfo(columnName)
		if columnDefine == nil {
			return nil, fmt.Errorf("column: %s does not exist", columnName)
		}
		if expr.CompareOp == token.EQUAL {
			return func(row ast.Row) bool {
				return row[i] == expr.RightVal
			}, nil
		} else if expr.CompareOp == token.NOT_EQUAL {
			return func(row ast.Row) bool {
				return row[i] != expr.RightVal
			}, nil
		} else {
			return nil, fmt.Errorf("compare operator: %v is not supported", expr.CompareOp)
		}
	} else {
		if expr.CompareOp == token.EQUAL {
			return func(row ast.Row) bool {
				return expr.LeftVal == expr.RightVal
			}, nil
		} else if expr.CompareOp == token.NOT_EQUAL {
			return func(row ast.Row) bool {
				return expr.LeftVal != expr.RightVal
			}, nil
		} else {
			return nil, fmt.Errorf("compare operator: %v is not supported", expr.CompareOp)
		}
	}
}

// @description: primary key equal search
func (dm *DataManager) pkEqSearch(rows chan<- ast.Row, index index.Index, expr ast.SQLSingleExpression) {
	// search page Num
	target := util.Uint32ToBytes(uint32(expr.RightVal.GetInt()))
	// pageNums is a channel([]byte) to store page numbers
	pageNums := index.Search(target)
	// set wait group
	wait := sync.WaitGroup{}
	wait.Add(util.Max_Parraled_Threads)
	// search pages in parallel
	for i := 0; i < util.Max_Parraled_Threads; i++ {
		go func() {
			defer wait.Done()
			for pageNo := range pageNums {
				pageNum := util.BytesToUInt32(pageNo)
				// get page
				recordPage, err := dm.pager.GetPage(pageNum, pagedata.NewRecordData())
				// get row data(page data)
				if err != nil {
					log.Errorf("get record page %d error: %s", pageNum, err)
				}
				pageData := recordPage.GetPageData().(*pagedata.RecordData)
				// get row data
				for i := range pageData.Rows() {
					row := pageData.Rows()[i]
					if row.GetPrimaryKey() == expr.RightVal {
						rows <- row
					}
				}
			}
		}()
	}
	go func() {
		// wait until all goroutines are done
		wait.Wait()
		close(rows)
	}()
}

// @description: search in non primary index
func (dm *DataManager) nPkEqSearch(rows chan<- ast.Row, npIndex index.Index, pIndex index.Index, expr ast.SQLSingleExpression) {
	// search page Num
	target := util.Uint32ToBytes(uint32(expr.RightVal.GetInt()))
	// pkNums is a channel([]byte) to store page numbers
	pkNums := npIndex.Search(target)
	// set wait group
	wait := sync.WaitGroup{}
	wait.Add(util.Max_Parraled_Threads)
	// search pages in parallel
	for i := 0; i < util.Max_Parraled_Threads; i++ {
		go func() {
			defer wait.Done()
			for pkNum := range pkNums {
				// get page numbers through primary index
				pageNumsChan := pIndex.Search(index.KeyType(pkNum))
				for pageNum := range pageNumsChan {
					pageNo := util.BytesToUInt32(pageNum)
					// get page
					recordPage, err := dm.pager.GetPage(pageNo, pagedata.NewRecordData())
					// get row data(page data)
					if err != nil {
						log.Errorf("get record page %d error: %s", pageNo, err)
					}
					pageData := recordPage.GetPageData().(*pagedata.RecordData)
					// get row data
					for i := range pageData.Rows() {
						row := pageData.Rows()[i]
						if row.GetPrimaryKey() == expr.RightVal {
							rows <- row
						}
					}
				}

			}
		}()
	}
	go func() {
		// wait until all goroutines are done
		wait.Wait()
		close(rows)
	}()
}

func (dm *DataManager) InsertData(insertStmt ast.SQLInsertStatement) error {
	// get table
	tableInfo := dm.pager.GetMetaData().GetTableInfo(insertStmt.TableName)
	if tableInfo == nil {
		return fmt.Errorf("table: %s does not exist", insertStmt.TableName)
	}
	// get table columns
	// set lens to nubmer of columns, and put columnDefine into the columns index
	var columns []*ast.SQLColumnDefine
	var columnIndexs []int
	// if sql statement has columns and check is datatype matched
	if len(insertStmt.ColumnNames) != 0 {
		for i, CN := range insertStmt.ColumnNames {
			if CN == "" {
				return fmt.Errorf("doesn't specify a column name")
			}
			index, columnDefine := tableInfo.GetColumnInfo(CN)
			if columnDefine == nil {
				return fmt.Errorf("column: %s does not exist", CN)
			}
			if columns[index] != nil {
				return fmt.Errorf("column: %s is duplicated", CN)
			}
			// check is column type match value type
			if ast.ValueTypeVsColumnType(insertStmt.Values[i].GetType(), columnDefine.ColumnType) {
				columns = append(columns, columnDefine)
				columnIndexs = append(columnIndexs, index)
			} else {
				return fmt.Errorf("column: %s type does not match value type", CN)
			}
		}
	} else {
		columns = tableInfo.GetColumns()
		for i := range columns {
			columnIndexs = append(columnIndexs, i)
		}
	}
	// find a suitable page(last page or a new page) to insert new row
	recordPage, err := dm.pager.SelectPage(int(insertStmt.ValueSize()), insertStmt.TableName)
	if err != nil {
		return fmt.Errorf("select page error: %s", err)
	}
	recordPageData := recordPage.GetPageData().(*pagedata.RecordData)
	// create a new row
	row := recordPageData.NewRow()
	// write data to row
	row.SetRowData(columnIndexs, insertStmt.Values)
	// add row to page
	recordPageData.AppendData(row)

	// update index
	for i := range columns {
		index := columns[i].Index
		if index != nil {
			// if column is primary key
			if columnIndexs[i] == 0 {
				index.Insert(row[0].Row(), util.Uint32ToBytes(recordPage.PageNo))
			} else {
				index.Insert(row[columnIndexs[i]].Row(), row[0].Row())
			}
		}
	}
	return nil
}
