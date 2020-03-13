package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/hyperledger/fabric/core/chaincode/shim"
)

var logger = shim.NewLogger("energy_trading")

const (
	tableName = "Meters"
)

type MeterInfo struct {
	Id             string  `json:"id"`
	Name           string  `json:"name"`
	Kwh            int64   `json:"kwh"`
	AccountBalance float64 `json:"account_balance"`
	RatePerKwh     int64   `json:"rate_per_kwh"`
}

type ByRate []*MeterInfo

func (a ByRate) Len() int {
	return len(a)
}

func (a ByRate) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByRate) Less(i, j int) bool {
	return a[i].RatePerKwh < a[j].RatePerKwh
}

// EnergyTradingChainCode implementation. This smart contract enables multiple smart meters
// to enroll and report their production/consumption of energy. It then lets user settle
// their accounts by moving funds from consumers to producers.
type EnergyTradingChainCode struct {
}

func (t *EnergyTradingChainCode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	var err error
	var val float64

	if len(args) == 0 {
		logger.Error("Incorrect number of arguments")
		return nil, errors.New("Incorrect number of arguments. Specify the exchange rate for this smart contract.")
	}

	val, err = strconv.ParseFloat(string(args[0]), 64)
	if err != nil {
		logger.Errorf("Invalid value %s for exchange rate", args[0])
		return nil, errors.New("Invalid value for exchange rate")
	}

	err = stub.PutState("exchange_rate", []byte(strconv.FormatFloat(val, 'f', 6, 64)))
	if err != nil {
		logger.Errorf("Error saving exchange rate %s", err.Error())
		return nil, errors.New("Exchange rate cannot be saved")
	}

	var exchangeAccountBalance float64
	exchangeAccountBalance = 0.0
	err = stub.PutState("exchange_account_balance", []byte(strconv.FormatFloat(exchangeAccountBalance, 'f', 6, 64)))
	if err != nil {
		logger.Errorf("Error saving exchange account balance %s", err.Error())
		return nil, errors.New("Exchange account balance cannot be saved")
	}

	_, err = stub.GetTable(tableName)
	if err == shim.ErrTableNotFound {
		err = stub.CreateTable(tableName, []*shim.ColumnDefinition{
			&shim.ColumnDefinition{Name: "AccountId", Type: shim.ColumnDefinition_STRING, Key: true},
			&shim.ColumnDefinition{Name: "AccountName", Type: shim.ColumnDefinition_STRING, Key: false},
			&shim.ColumnDefinition{Name: "ReportedKWH", Type: shim.ColumnDefinition_INT64, Key: false},
			&shim.ColumnDefinition{Name: "AccountBalance", Type: shim.ColumnDefinition_STRING, Key: false},
			&shim.ColumnDefinition{Name: "RatePerKWH", Type: shim.ColumnDefinition_INT64, Key: false},
		})
		if err != nil {
			logger.Errorf("Error creating table:%s", err.Error())
			return nil, errors.New("Failed creating AssetsOnwership table.")
		}
	} else {
		logger.Info("Table already exists")
	}

	logger.Info("Successfully deployed chain code")

	return nil, nil
}

func (t *EnergyTradingChainCode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

	if function == "enroll" {
		return t.enroll(stub, args)
	}

	if function == "delete" {
		return t.delete(stub, args)
	}

	if function == "changeAccountBalance" {
		return t.changeAccountBalance(stub, args)
	}

	if function == "reportDelta" {
		return t.reportDelta(stub, args)

	}

	if function == "settle" {
		return t.settle(stub, args)
	}

	logger.Errorf("Unimplemented method :%s called", function)

	return nil, errors.New("Unimplemented '" + function + "' invoked")
}

// Enrolls a new meter
func (t *EnergyTradingChainCode) enroll(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	logger.Info("In enroll function")
	if len(args) < 3 {
		logger.Error("Incorrect number of arguments")
		return nil, errors.New("Incorrect number of arguments. Specify account number, name and rate per kwh")
	}

	accountId := args[0]
	accountName := args[1]
	rateKwhStr := args[2]
	rateKwh, err := strconv.ParseInt(string(rateKwhStr), 10, 64)
	if err != nil {
		logger.Errorf("Error in converting to int:%s", err.Error())
		return nil, fmt.Errorf("Invalid value of rate per kwh:%s", rateKwhStr)
	}

	logger.Infof("Enrolling meter with id:%s, name:%s and target rate:%d", accountId, accountName, rateKwh)

	ok, err := stub.InsertRow(tableName, shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: accountId}},
			&shim.Column{Value: &shim.Column_String_{String_: accountName}},
			&shim.Column{Value: &shim.Column_Int64{Int64: 0}},
			&shim.Column{Value: &shim.Column_String_{String_: "0.0"}},
			&shim.Column{Value: &shim.Column_Int64{Int64: rateKwh}},
		},
	})

	if !ok && err == nil {
		logger.Errorf("Error in enrolling a new account:%s", err)
		return nil, errors.New("Error in enrolling a new account")
	}
	logger.Infof("Enrolled account %s", accountId)

	return nil, nil
}

// Deletes an existing meter
func (t *EnergyTradingChainCode) delete(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	logger.Info("In delete function")
	if len(args) != 1 {
		logger.Error("Incorrect number of arguments")
		return nil, errors.New("Incorrect number of arguments. Specify account number to be deleted")
	}

	accountId := args[0]

	logger.Infof("Deleting meter with id:%s", accountId)

	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: accountId}}
	columns = append(columns, col1)
	err := stub.DeleteRow(tableName, columns)
	if err != nil {
		logger.Errorf("Error in deleting an account:%s", err)
		return nil, errors.New("Error in deleting an account")
	}
	logger.Infof("Deleted account %s", accountId)

	return nil, nil
}

func (t *EnergyTradingChainCode) getRow(stub shim.ChaincodeStubInterface, accountId string) (shim.Row, error) {
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: accountId}}
	columns = append(columns, col1)

	return stub.GetRow(tableName, columns)
}

func (t *EnergyTradingChainCode) updateRow(stub shim.ChaincodeStubInterface, row shim.Row) (bool, error) {
	return stub.ReplaceRow(tableName, row)
}

// Change account balance. +ve value means deposit and -ve value means withdrawal
func (t *EnergyTradingChainCode) changeAccountBalance(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	logger.Info("In changeAccountBalance function")
	if len(args) < 2 {
		logger.Error("Incorrect number of arguments")
		return nil, errors.New("Incorrect number of arguments. Specify account number and fund to be deposited")
	}

	accountId := args[0]
	amountToBeDeposited := args[1]

	logger.Debugf("Adding %s coins to meter with id:%s", amountToBeDeposited, accountId)
	numCoins, err := strconv.ParseFloat(string(amountToBeDeposited), 64)
	if err != nil {
		logger.Errorf("Error in converting to float:%s", err.Error())
		return nil, fmt.Errorf("Invalid value of amount to be deposited:%s", amountToBeDeposited)
	}

	row, err := t.getRow(stub, accountId)
	if err != nil {
		logger.Errorf("Failed retrieving account [%s]: [%s]", accountId, err)
		return nil, fmt.Errorf("Failed retrieving account [%s]: [%s]", accountId, err)
	}
	prevBalanceStr := row.Columns[3].GetString_()
	logger.Debugf("Previous balance for account:%s is %s", accountId, prevBalanceStr)
	prevBalance, err := strconv.ParseFloat(string(prevBalanceStr), 64)
	if err != nil {
		logger.Errorf("Error in converting to float:%s", err.Error())
		return nil, fmt.Errorf("Invalid value of accountBalance:%s", prevBalanceStr)
	}
	newBalance := prevBalance + numCoins
	logger.Debugf("New balance for account:%s is %f", accountId, newBalance)
	newBalanceStr := strconv.FormatFloat(newBalance, 'f', 6, 64)
	row.Columns[3] = &shim.Column{Value: &shim.Column_String_{String_: newBalanceStr}}

	ok, err := t.updateRow(stub, row)
	if !ok && err == nil {
		logger.Errorf("Error in updating account:%s with balance:%s", accountId, newBalanceStr)
		return nil, errors.New("Error in updating account")
	}
	logger.Infof("Changed account balance for account: %s", accountId)

	return nil, nil
}

// Report energy produced or consumed. +ve value means produced and -ve value means consumed
func (t *EnergyTradingChainCode) reportDelta(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	logger.Info("In reportDelta function")
	if len(args) < 2 {
		logger.Error("Incorrect number of arguments")
		return nil, errors.New("Incorrect number of arguments. Specify account number and fund to be deposited")
	}

	accountId := args[0]
	amountKwhReported := args[1]

	logger.Debugf("Accumulating energy reported %s kwh to meter with id:%s", amountKwhReported, accountId)
	reportedKwhDelta, err := strconv.ParseInt(string(amountKwhReported), 10, 64)
	if err != nil {
		logger.Errorf("Error in converting to int:%s", err.Error())
		return nil, fmt.Errorf("Invalid value of reported kwh to be accumulated:%s", amountKwhReported)
	}

	row, err := t.getRow(stub, accountId)
	if err != nil {
		logger.Errorf("Failed retrieving account [%s]: [%s]", accountId, err)
		return nil, fmt.Errorf("Failed retrieving account [%s]: [%s]", accountId, err)
	}
	prevBalance := row.Columns[2].GetInt64()
	logger.Debugf("Previous reported kwh for account:%s is %d", accountId, prevBalance)
	newBalance := prevBalance + reportedKwhDelta
	logger.Debugf("New reported kwh for account:%s is %d", accountId, newBalance)
	row.Columns[2] = &shim.Column{Value: &shim.Column_Int64{Int64: newBalance}}

	ok, err := t.updateRow(stub, row)
	if !ok && err == nil {
		logger.Errorf("Error in updating reported kwh:%s with balance:%d", accountId, newBalance)
		return nil, errors.New("Error in updating account")
	}
	logger.Infof("Changed reported kwh for account: %s", accountId)

	return nil, nil
}

// Settles the accounts and resets the reported kwh back to 0 for all meters
func (t *EnergyTradingChainCode) settle(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	logger.Info("In settle function")
	var columns []shim.Column

	rowChannel, err := stub.GetRows(tableName, columns)
	if err != nil {
		logger.Errorf("Error in getting rows:%s", err.Error())
		return nil, errors.New("Error in fetching rows")
	}
	meters := make([]*MeterInfo, 0)
	for row := range rowChannel {
		balance, err := strconv.ParseFloat(row.Columns[3].GetString_(), 64)
		if err != nil {
			logger.Errorf("Error in converting to float:%s", err.Error())
			return nil, fmt.Errorf("Invalid value of accountBalance:%s", row.Columns[3].GetString_())
		}
		meter := MeterInfo{
			Id:             row.Columns[0].GetString_(),
			Name:           row.Columns[1].GetString_(),
			Kwh:            row.Columns[2].GetInt64(),
			AccountBalance: balance,
			RatePerKwh:     row.Columns[4].GetInt64(),
		}
		meters = append(meters, &meter)
	}
	logger.Infof("Number of rows in table:%d", len(meters))

	xchngRateStr, err := stub.GetState("exchange_rate")
	if err != nil {
		logger.Error("Failed to retrieve exchange rate")
		return nil, fmt.Errorf("Failed to retrieve exchange rate")
	}

	xchngRate, err := strconv.ParseFloat(string(xchngRateStr), 64)
	if err != nil {
		logger.Errorf("Invalid value %s for exchange rate", xchngRateStr)
		return nil, errors.New("Invalid value for exchange rate")
	}
	logger.Debugf("Smart contract will charge producers at rate of %f", xchngRate)

	xchngBalanceStr, err := stub.GetState("exchange_account_balance")
	if err != nil {
		logger.Error("Failed to retrieve exchange account balance")
		return nil, fmt.Errorf("Failed to retrieve exchange account balance")
	}

	xchngBalance, err := strconv.ParseFloat(string(xchngBalanceStr), 64)
	if err != nil {
		logger.Errorf("Invalid value %s for exchange account balance", xchngBalanceStr)
		return nil, errors.New("Invalid value for exchange account balance")
	}

	logger.Debug("Seggregating buyers and sellers")
	buyers := make([]*MeterInfo, 0)
	sellers := make([]*MeterInfo, 0)
	for _, meter := range meters {
		if meter.Kwh < 0 {
			logger.Debugf("Meter %s is a buyer", meter.Id)
			buyers = append(buyers, meter)
		} else {
			logger.Debugf("Meter %s is a seller", meter.Id)
			sellers = append(sellers, meter)
		}
	}
	// Sort the buyers so we can match buyers with lower asking rate with sellers offering
	// lower rates
	sort.Sort(ByRate(buyers))
	// Sort the sellers so buyers can purchase from sellers offering lower rates first
	sort.Sort(ByRate(sellers))

	logger.Infof("Number of buyers: %d, number of sellers: %d", len(buyers), len(sellers))
	for _, buyer := range buyers {
		logger.Debugf("Finding sellers for buyer:%s with rate less than %d for %d KWH", buyer.Id, buyer.RatePerKwh, buyer.Kwh)
		// Very crude way of setteling...O(n^2) complexity...need to improve
		for _, seller := range sellers {
			if buyer.Kwh == 0 {
				logger.Debugf("Buyer %s has all its energy need satisfied", buyer.Id)
				break
			}
			if seller.RatePerKwh <= buyer.RatePerKwh && seller.Kwh > 0 {
				logger.Debugf("Seller %s has produced %d at rate less or equal to buyer's requirement", seller.Id, seller.Kwh)
				energyConsumed := buyer.Kwh * -1
				if energyConsumed <= seller.Kwh {
					seller.Kwh = seller.Kwh - energyConsumed
					// Set the energy consumed by buyer to 0
					buyer.Kwh = 0
					amountDebited := float64(energyConsumed * seller.RatePerKwh)
					buyer.AccountBalance = buyer.AccountBalance - amountDebited
					feeAssessed := amountDebited * xchngRate
					xchngBalance = xchngBalance + feeAssessed
					amountCredited := amountDebited - feeAssessed
					logger.Debugf("Amount debited from buyer %s is %f and amount credited to seller %s is %f", buyer.Id, amountDebited, seller.Id, amountCredited)
					logger.Debugf("Fee charged for this transaction: %f", feeAssessed)
					seller.AccountBalance = seller.AccountBalance + amountCredited
				} else {
					logger.Debugf("Only partial need of buyer %s is satisfied by seller %s", buyer.Id, seller.Id)
					// Add seller Kwh to buyer, which will essentially reduce buyer Kwh consumption
					// as buyer Kwh is -ve
					buyer.Kwh = buyer.Kwh + seller.Kwh
					partialEnergyConsumed := seller.Kwh
					logger.Debugf("Total unsatisfied energy need for buyer:%s is %d", buyer.Id, buyer.Kwh)
					// Set the energy produced by seller to 0
					seller.Kwh = 0
					amountDebited := float64(partialEnergyConsumed * seller.RatePerKwh)
					buyer.AccountBalance = buyer.AccountBalance - amountDebited
					feeAssessed := amountDebited * xchngRate
					xchngBalance = xchngBalance + feeAssessed
					amountCredited := amountDebited - feeAssessed
					logger.Debugf("Amount debited from buyer %s is %f and amount credited to seller %s is %f", buyer.Id, amountDebited, seller.Id, amountCredited)
					logger.Debugf("Fee charged for this transaction: %f", feeAssessed)
					seller.AccountBalance = seller.AccountBalance + amountCredited
				}
			}
		}
	}
	// Now update the table
	for _, meter := range meters {
		row, err := t.getRow(stub, meter.Id)
		if err != nil {
			logger.Errorf("Failed retrieving account [%s]: [%s]", meter.Id, err)
			return nil, fmt.Errorf("Failed retrieving account [%s]: [%s]", meter.Id, err)
		}

		newBalanceStr := strconv.FormatFloat(meter.AccountBalance, 'f', 6, 64)
		row.Columns[3] = &shim.Column{Value: &shim.Column_String_{String_: newBalanceStr}}
		row.Columns[2] = &shim.Column{Value: &shim.Column_Int64{Int64: meter.Kwh}}

		ok, err := t.updateRow(stub, row)
		if !ok && err == nil {
			logger.Errorf("Error in settling account:%s", meter.Id)
			return nil, errors.New("Error in settling account")
		}
	}

	logger.Debugf("New balance for exchange account: %f", xchngBalance)
	err = stub.PutState("exchange_account_balance", []byte(strconv.FormatFloat(xchngBalance, 'f', 6, 64)))
	if err != nil {
		logger.Errorf("Error saving exchange account balance %s", err.Error())
		return nil, errors.New("Exchange account balance cannot be saved")
	}
	logger.Info("Done settling")

	return nil, nil
}

// Query callback representing the query of a chaincode
func (t *EnergyTradingChainCode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

	if function == "balance" {
		return t.balance(stub, args)
	}

	if function == "reportedKwh" {
		return t.reportedKwh(stub, args)
	}

	if function == "exchangeRate" {
		return t.exchangeRate(stub, args)
	}

	if function == "exchangeAccountBalance" {
		return t.exchangeAccountBalance(stub, args)
	}

	if function == "meterInfo" {
		return t.meterInfo(stub, args)
	}

	if function == "meters" {
		return t.meters(stub, args)
	}

	return nil, errors.New("Invalid query function name")
}

// Return reported kwh
func (t *EnergyTradingChainCode) reportedKwh(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	logger.Info("In reportedKwh function")
	if len(args) == 0 {
		logger.Error("Incorrect number of arguments")
		return nil, errors.New("Incorrect number of arguments. Specify account number")
	}

	accountId := args[0]

	logger.Debugf("Getting reported kwh for meter with id:%s", accountId)

	row, err := t.getRow(stub, accountId)
	if err != nil {
		logger.Errorf("Failed retrieving account [%s]: [%s]", accountId, err)
		return nil, fmt.Errorf("Failed retrieving account [%s]: [%s]", accountId, err)
	}
	reportedKwh := row.Columns[2].GetInt64()
	logger.Debugf("Reported KWH for account:%s is %d", accountId, reportedKwh)
	reportedKwhStr := strconv.FormatInt(reportedKwh, 10)

	return []byte(reportedKwhStr), nil
}

func (t *EnergyTradingChainCode) balance(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	logger.Info("In balance function")
	if len(args) == 0 {
		logger.Error("Incorrect number of arguments")
		return nil, errors.New("Incorrect number of arguments. Specify account number")
	}

	accountId := args[0]

	logger.Debugf("Getting account balance for meter with id:%s", accountId)

	row, err := t.getRow(stub, accountId)
	if err != nil {
		logger.Errorf("Failed retrieving account [%s]: [%s]", accountId, err)
		return nil, fmt.Errorf("Failed retrieving account [%s]: [%s]", accountId, err)
	}
	balance := row.Columns[3].GetString_()
	logger.Debugf("Account balance for account:%s is %s", accountId, balance)

	return []byte(balance), nil
}

// Return meter information
func (t *EnergyTradingChainCode) meterInfo(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	logger.Info("In reportedKwh function")
	if len(args) == 0 {
		logger.Error("Incorrect number of arguments")
		return nil, errors.New("Incorrect number of arguments. Specify account number")
	}

	accountId := args[0]

	logger.Debugf("Getting reported kwh for meter with id:%s", accountId)

	row, err := t.getRow(stub, accountId)
	if err != nil {
		logger.Errorf("Failed retrieving account [%s]: [%s]", accountId, err)
		return nil, fmt.Errorf("Failed retrieving account [%s]: [%s]", accountId, err)
	}

	balance, err := strconv.ParseFloat(row.Columns[3].GetString_(), 64)
	if err != nil {
		logger.Errorf("Error in converting to float:%s", err.Error())
		return nil, fmt.Errorf("Invalid value of accountBalance:%s", row.Columns[3].GetString_())
	}

	meter := MeterInfo{
		Id:             row.Columns[0].GetString_(),
		Name:           row.Columns[1].GetString_(),
		Kwh:            row.Columns[2].GetInt64(),
		AccountBalance: balance,
		RatePerKwh:     row.Columns[4].GetInt64(),
	}

	payload, err := json.Marshal(meter)
	if err != nil {
		logger.Errorf("Failed retrieving account [%s]: [%s]", accountId, err)
		return nil, fmt.Errorf("Failed marshalling payload [%s]: [%s]", accountId, err)
	}

	return payload, nil
}

// Return all meters
func (t *EnergyTradingChainCode) meters(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	logger.Info("In reportedKwh function")
	if len(args) > 0 {
		logger.Error("Incorrect number of arguments")
		return nil, errors.New("Incorrect number of arguments. No arguments required")
	}

	var columns []shim.Column

	rowChannel, err := stub.GetRows(tableName, columns)
	if err != nil {
		logger.Errorf("Error in getting rows:%s", err.Error())
		return nil, errors.New("Error in fetching rows")
	}
	meters := make([]MeterInfo, 0)
	for row := range rowChannel {
		balance, err := strconv.ParseFloat(row.Columns[3].GetString_(), 64)
		if err != nil {
			logger.Errorf("Error in converting to float:%s", err.Error())
			return nil, fmt.Errorf("Invalid value of accountBalance:%s", row.Columns[3].GetString_())
		}
		meter := MeterInfo{
			Id:             row.Columns[0].GetString_(),
			Name:           row.Columns[1].GetString_(),
			Kwh:            row.Columns[2].GetInt64(),
			AccountBalance: balance,
			RatePerKwh:     row.Columns[4].GetInt64(),
		}
		meters = append(meters, meter)
	}

	payload, err := json.Marshal(meters)
	if err != nil {
		logger.Errorf("Failed marshalling payload")
		return nil, fmt.Errorf("Failed marshalling payload [%s]", err)
	}

	return payload, nil
}

func (t *EnergyTradingChainCode) exchangeRate(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	logger.Info("In exchangeRate function")
	if len(args) > 0 {
		logger.Error("Incorrect number of arguments")
		return nil, errors.New("Incorrect number of arguments. No arguments necessary")
	}

	xchngRate, err := stub.GetState("exchange_rate")
	if err != nil {
		logger.Error("Failed to retrieve exchange rate")
		return nil, fmt.Errorf("Failed to retrieve exchange rate")
	}

	return xchngRate, nil
}

func (t *EnergyTradingChainCode) exchangeAccountBalance(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	logger.Info("In exchangeAccountBalance function")
	if len(args) > 0 {
		logger.Error("Incorrect number of arguments")
		return nil, errors.New("Incorrect number of arguments. No arguments necessary")
	}

	xchngRate, err := stub.GetState("exchange_account_balance")
	if err != nil {
		logger.Error("Failed to retrieve exchange account balance")
		return nil, fmt.Errorf("Failed to retrieve exchange account balance")
	}

	return xchngRate, nil
}

func main() {
	err := shim.Start(new(EnergyTradingChainCode))
	if err != nil {
		fmt.Printf("Error starting Energy trading chaincode: %s", err)
	}
}
