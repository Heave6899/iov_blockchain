package main

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/protos/peer"
	"math"
)

func main() {
	vehicleArray := []Vehicle{}

	fmt.Print(vehicleArray)
}

type Vehicle struct {
	modelCompany string 'json:"company"'
	modelType    string 'json:"type"'
	modelName    string 'json:"name"'
	modelID      float64 'json:"Id"'
	owner        Owner 
	gps          GPS
}

type Owner struct {
	fName      string 'json:"fName"'
	lName      string 'json:"lName"'
	licenseNum string 'json:"licenseNum"'
	insurance  string 'json:"insurance"'
	gender     string 'json:"gender"'
	phoneNo    string 'json:"phoneNo"'
	address    string 'json:"address"'
}
type GPS struct {
	lat  float64 'json:"lat"'
	long float64 'json:"long"'
}

"Init..."
func (c *Vehicle) Init(stub shim.ChaincodeStubInterface) pb.Response {
	func (c *AssetMgr) Init(stub shim.ChaincodeStubInterface) pb.Response { args := stub.GetStringArgs()

		if len(args) != 11 {
		return shim.Error(“Incorrect arguments. Expecting a key and a value”)}
		modelcompany := args[0]
		modeltype := args[1]
		modelname := args[2]
		modelid := args[3]
		fname := args[4]
		lname := args[5]
		licensenum := args[6]
		insure := args[7]
		gen := args[8]
		phoneno := args[9]
		add := args[10]
		//create asset
		assetData := Vehicle{
			modelCompany : modelcompany,
			modelType : modeltype,
			modelID : modelid,
			modelName : modelname,
		}
		assetData.owner = Owner{
			fName:      fname,
			lName:      lname,
			licenseNum: licensenum,
			insurance:  insure,
			gender:     gen,
			phoneNo:    phoneno,
			address:    add,
		}
		assetBytes, _ := json.Marshal(assetData) assetErr := stub.PutState(assetId, assetBytes) 
		if assetErr != nil {
		return shim.Error(fmt.Sprintf(“Failed to create asset: %s”, args[0]))
		}
	return shim.Success(nil)
}

"Invoke..."
func (c *Vehicle) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	function, args := stub.GetFunctionAndParameters()
	if function == “Order” {
	return c.Order(stub, args)       
	} else if function == “Request” {   
	return c.Request(stub, args) 
	} else if function == “RetrieveStatusSOC” {    
	return c.RetrieveStatusSOC(stub, args)  
	} else if function == “getVehicle” {  
	return c.getVehicle(stub, args)
	} else if function == “getAllVehicle” {    
	return c.getAllVehicle(stub, args)  
	}             
	return shim.Error(“Invalid function name”)
}

func (c *Vehicle) shortestpath(stub shim.ChaincodeStubInterface, args	[]string) pb.Response {

}

func addNewPeer(vehicleArray []Vehicle) {
	var V Vehicle = newPeer()
	vehicleArray = append(vehicleArray, V)
}

func shortestpath(vehicleArray *[]Vehicle) {
	queueShortvehicles := []float64{}
	for i := range vehicleArray {
		x := algoshortPath(vehicleArray[i].gps.lat, vehicleArray[i+1].gps.lat, vehicleArray[i].gps.long, vehicleArray[i+1].gps.long)
		if x < 4 {
			queueShortvehicles = append(queueShortvehicles, float64(i), float64(i+1))
		}
	}
}

func algoshortPath(lat1 float64, lat2 float64, long1 float64, long2 float64) float64 {
	var pi80 float64 = math.Pi / 180
	lat1 *= pi80
	long1 *= pi80
	lat2 *= pi80
	long2 *= pi80

	var r float64 = 6372.797 // mean radius of Earth in km
	var dlat float64 = lat2 - lat1
	var dlong float64 = long2 - long1
	var a float64 = math.Sin(dlat/2)*math.Sin(dlat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dlong/2)*math.Sin(dlong/2)
	var c float64 = 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	var km float64 = r * c

	return km * 0.68953
}

func main() {
	err := shim.Start(new(Vehicle))
	if err != nil {
		fmt.Printf("Error creating new Vehicle Contract: %s", err)
	}