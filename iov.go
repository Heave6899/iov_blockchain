package main

import (
	"fmt"
	"math"
)

func main() {
	vehicleArray := []Vehicle{}

	fmt.Print(vehicleArray)
}

type Vehicle struct {
	modelCompany string
	modelType    string
	modelName    string
	modelID      float64
	owner        Owner
	gps          GPS
}

type Owner struct {
	fName      string
	lName      string
	licenseNum string
	insurance  string
	gender     string
	phoneNo    string
	address    string
}
type GPS struct {
	lat  float64
	long float64
}

func newPeer() Vehicle {
	var modelCompany, modelType, modelName, fName, lName, licenseNum, insurance, g, phno, add string
	var modelID, lat, long float64
	fmt.Scan(&modelCompany)
	fmt.Scan(&modelType)
	fmt.Scan(&modelName)
	fmt.Scan(&modelID)
	fmt.Scan(&fName)
	fmt.Scan(&lName)
	fmt.Scan(&licenseNum)
	fmt.Scan(&insurance)
	fmt.Scan(&g)
	fmt.Scan(&phno)
	fmt.Scan(&add)
	fmt.Scan(&lat)
	fmt.Scan(&long)
	//fmt.Print("It's done")
	v := Vehicle{
		modelCompany: modelCompany,
		modelType:    modelType,
		modelName:    modelName,
		modelID:      modelID}
	v.gps = GPS{
		lat:  lat,
		long: long,
	}
	v.owner = Owner{
		fName:      fName,
		lName:      lName,
		licenseNum: licenseNum,
		insurance:  insurance,
		gender:     g,
		phoneNo:    phno,
		address:    add,
	}
	//fmt.Print("function Exit")
	//fmt.Print(v)
	return v
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
