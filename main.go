package main



import  (
	"strings"
	"io/ioutil"
	"fmt"
	"encoding/json"
	"os"
	"net/http"
	"log"
	"github.com/jamespearly/loggly"
	"strconv"
	"time"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	
)

/*
AWS Credentials
*/

 
/*type Trip struct {
	Trip_id string `json:"trip_id"`
}

type Trip_Update struct {
	Key Trip
}

type Entity struct {
	Id string `json:"id"`
	Key Trip_Update  `json:"trip_update"`
	
}

type Data struct {
	Key string `json:"data"`
}

*/

var client loggly.ClientType //client which logs messages

type Arrival struct {
	Delay int `json:"delay"`
}

type Stop_time_update struct {
	Stop_sequence int `json:"stop_sequence"`
	Stop_id string `json:"stop_id"`
	Arrival Arrival `json:"arrival"`
	
}

type Trip struct {
	Trip_id string `json:"trip_id"`
	Start_date string `json:"start_date"`
	
}
type Trip_update struct {
	Trip Trip `json:'"trip""`
	Stop_time_update []Stop_time_update `json:"stop_time_update"`

}
type Entity struct {
	Id string `json:"id"`
	Trip_update Trip_update `json:"trip_update"`
}
type Data struct {
	Entity []Entity `json:"entity"`
}
type Info struct {
	Status bool `json:"status"`
//	Route string `json:"route"`
	Data Data `json:"data"`
}

func init_loggly() {
	client:= loggly.New("Train-App")
	client.EchoSend("info", "Application instance started...")
}

func make_request () []byte {
	logger := loggly.New("Train-App")
	url := "https://mnorth.prod.acquia-sites.com/wse/LIRR/gtfsrt/realtime/"

	request_url :=  url + os.Getenv("API_KEY") + "/json"
	fmt.Printf("Request Url: %s\n", request_url)
	resp, err := http.Get(request_url)
	if(err != nil) {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	
	if err !=nil {
		logger.EchoSend("error", "Http Read Failed")
	}
		size := cap(body)
		message := strconv.Itoa(size) + " bytes read"
		logger.EchoSend("info", message)
	fmt.Println(message)
		err2 := logger.EchoSend("debug", "HTTP request and read Successful")
	fmt.Println(err2)

	//_, err = os.Stdout.Write(body)
	return body

}

func get_train (jsonBody []byte) Info{
	info := Info{}
//	var result map[string]interface{}
	jsonErr := json.Unmarshal(jsonBody,&info)
	if jsonErr != nil {
		//log.Fatal(jsonErr)
//		fmt.Println("No")
		client.EchoSend("error", "JSON parsing failed")
	}else {
		client.EchoSend("debug", "JSON successfully parsed")
	}
//	fmt.Println((result["data"].(map[string]interface{}))["entity"][0])
//	fmt.Println(info)
	return info

}

func display_train_info(info Info) {
	client:= loggly.New("Train-App")
	var output strings.Builder
	output.WriteString("status: ")
	output.WriteString(strconv.FormatBool(info.Status))
	client.EchoSend("info", output.String())
	fmt.Println("data: { entity: [")
	entity := info.Data.Entity;
	for i:=0; i < len(entity); i++ {
		if(entity[i].Id[len(entity[i].Id) - 1] == 'V' ) {
			continue
		}
		fmt.Println("\t{id: " 
+ entity[i].Id)
		fmt.Println("\t\ttrip_update: {")
		fmt.Println("\t\t\ttrip: {")
		fmt.Println("\t\t\t\ttrip_id: " + entity[i].Trip_update.Trip.Trip_id)
		fmt.Println("\t\t\t\tstart_date: " + entity[i].Trip_update.Trip.Start_date)
		fmt.Println("\t\t\t}")
		fmt.Println("\t\t\tstop_time_update: [")
		for j:=0; j < len(entity[i].Trip_update.Stop_time_update); j++ {
			fmt.Println("\t\t\t{\tstop_sequence:",entity[i].Trip_update.Stop_time_update[j].Stop_sequence)
			fmt.Println("\t\t\t\tstop_id: " + entity[i].Trip_update.Stop_time_update[j].Stop_id)
			fmt.Println("\t\t\t\tdelay: ", entity[i].Trip_update.Stop_time_update[j].Arrival.Delay)
			fmt.Println("\t\t\t}")

		}
		fmt.Println("\t\t\t}}}]}")
	}
	
	client.EchoSend("debug", "Finished printing parsed json")
}
/*func key_creator(primary_hash_key string) *dynodb.key {
	key := &dynamodb.Key {
		HashKey: msid
	}
	return key
}*/
func connect_db() *dynamodb.DynamoDB{
	sess, err :=session.NewSession(&aws.Config {
		Region: aws.String("us-east-1"),
	})
//	fmt.Println("Session: ", sess, err)

	svc := dynamodb.New(sess)
	result, err := svc.ListTables(&dynamodb.ListTablesInput{})
	
	fmt.Println("Result ", result, " Error ", err)
	if err != nil {
		fmt.Println("Tables:")
		fmt.Println("")
		for _, n := range result.TableNames {
			fmt.Println(*n)
		
		}
	}
	return svc
}


func main () {
	var svc = connect_db()
	init_loggly()
	for {
		time.Sleep(30 * time.Second)
		var jsonBody = make_request()
		info := get_train(jsonBody)
		display_train_info(info)
		
		if(len(info.Data.Entity) == 0) {
			continue
		}
		entity:= info.Data.Entity
		for i:=0; i < len(entity); i++ {
			if(entity[i].Id[len(entity[i].Id) - 1] == 'V' ) {
				continue
			}
			avMap, err :=dynamodbattribute.MarshalMap(info.Data.Entity[i])
			input := &dynamodb.PutItemInput {
				Item: avMap,
				TableName: aws.String("lirr_data"),
			}
			_, err = svc.PutItem(input)
			if err != nil {
				fmt.Println("Error calling putitem: ")
				fmt.Println(err.Error())
				os.Exit(1)
			} else {
				fmt.Println("Wrote Data to Table")
			}
		}
	}
	
}
//	client.EchoSend("info", "Application instance successfully terminated without errors.")

