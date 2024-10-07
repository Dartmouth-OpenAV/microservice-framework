//version 1.1.6

package framework

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo"
)

// tunables
// Use setFrameworkGlobals() in microservice.go to change these variables.
var UseUDP = false
var UseTelnet = false
var KeepAlive = false
var ConnectTimeout = 5          // seconds
var ReadTimeout = 5             // seconds
var TryReadTimeout = 150        // milliseconds
var WriteTimeout = 5            // seconds
var RefreshDataPointsEvery = 50 // seconds (should be 50 after debugging so it's a little faster than the orchestrator gets)
var KeepRefreshingFor = 300     // seconds (should be 300 after debugging)
var ReadNoSleepTries = 5
var MaxReadTries = 6
var ReadFailSleep = 500 // milliseconds
var FunctionStackFull = 100
var ReportOlderInterval = 120 // seconds (used to throttle function stack errors when network connectivity is gone)
var DefaultSocketPort int
var MicroserviceName = ""

// All possible values are "Don't add another instance", "Remove older instance", or "Add another instance"
// microservices can override this in their own code by setting this global variable
var CheckFunctionAppendBehavior string = "Don't add another instance"
var GlobalDelimiter = 13 // Read end of line delimiter (defaults to carriage return if the microservice doesn't set it)

// globals
// We've found sync.Map that could handle locking/unlocking by itself. Keeping as is for now to avoid potentially introducing bugs.
var RelatedActions = make(map[string]map[string]string)
var deviceStates = make(map[string]map[string]string)
var deviceStatesMutex sync.Mutex
var deviceErrors = make(map[string]map[string]string)
var deviceErrorsMutex sync.Mutex
var lastQueried = make(map[string]time.Time)
var lastQueriedMutex sync.Mutex
var connectionsUDP = make(map[string]*net.UDPConn)
var connectionsTCP = make(map[string]*net.TCPConn)

// var connections = make(map[string]*net.Conn) // we'll set this to one of the above at runtime
var connectionsMutex sync.Mutex
var deviceMutexes = make(map[string]sync.Mutex)
var devicesLock = make(map[string]bool)
var devicesLockMutex sync.Mutex

var global_reader = make(map[string]*bufio.Reader)

// function stack
var functionStack = [][]string{}
var functionStackTimes = []time.Time{}
var functionStackMutex sync.Mutex

var deviceMutex sync.Mutex

// Define a type for the callback function
type MainGetFunc func(socketKey string, setting string, arg1 string, arg2 string) (string, error)
type MainSetFunc func(socketKey string, setting string, arg1 string, arg2 string, arg3 string) (string, error)

var doDeviceSpecificGet MainGetFunc
var doDeviceSpecificSet MainSetFunc

// taken from: https://medium.com/@vicky.kurniawan/go-call-a-function-from-string-name-30b41dcb9e12
type stubMapping map[string]interface{}

var stubStorage = stubMapping{}

// Function to register the main package function
func RegisterMainGetFunc(fn MainGetFunc) {
	doDeviceSpecificGet = fn
}

// Function to register the main package function
func RegisterMainSetFunc(fn MainSetFunc) {
	doDeviceSpecificSet = fn
}

func registerMicroserviceFunctions(router *echo.Echo) {
	router.GET("/", index)
	router.GET(":address/errors", getErrors)
	router.GET(":address", handleGet)
	router.GET(":address/:setting", handleGet)
	router.GET(":address/:setting/:arg1", handleGet)
	router.GET(":address/:setting/:arg1/:arg2", handleGet)
	router.PUT(":address", handleUpdate)
	router.PUT(":address/:setting", handleUpdate)
	router.PUT(":address/:setting/:arg1", handleUpdate)
	router.PUT(":address/:setting/:arg1/:arg2", handleUpdate)

	// make sure all functions meant to be stacked are listed here
	stubStorage = map[string]interface{}{
		"endPointRefresh": endPointRefresh,
		"updateDoOnce":    updateDoOnce,
	}
}

func index(context echo.Context) error {
	return context.String(http.StatusOK, "\""+MicroserviceName+"\"\n")
}

func Startup() {
	function := "Startup"
	defer func() {
		if r := recover(); r != nil {
			retMsg := fmt.Sprintf(function+" - asdf54dgf unhandled panic: %v stack: %v", r, string(debug.Stack()[:]))
			AddToErrors("all", retMsg)
		}
	}()

	Log(MicroserviceName + " using OpenAV microservice framework")

	// echo instance
	router := echo.New()

	// middleware
	// router.Use( middleware.Logger() ) // This is verbose but reveals useful information
	// router.Use(middleware.Recover())

	Log("Starting function stack processor")
	go functionStackProcessor()

	Log("Registering endpoint functions")
	registerMicroserviceFunctions(router)

	// start server
	Log("Starting web server")
	router.Logger.Fatal(router.Start(":80"))
}

func call(socketKey string, funcName string, params ...interface{}) (result interface{}, err error) {
	function := "call"
	Log(fmt.Sprintf(function+" - calling: "+funcName+" with %v", params))

	f := reflect.ValueOf(stubStorage[funcName])
	if len(params) != f.Type().NumIn() {
		Log(fmt.Sprintf("got %d parameters, expected %d", len(params), f.Type().NumIn()))
		err = errors.New("number of parameters is out of index")
		devicesLockMutex.Lock()
		devicesLock[socketKey] = false
		devicesLockMutex.Unlock()
		return
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	var res []reflect.Value
	res = f.Call(in)
	result = res[0].Interface()

	var boolResult bool
	boolResult = result.(bool)
	if boolResult == false {
		logError(function + " - uh oh it didn't work")
	}

	devicesLockMutex.Lock()
	devicesLock[socketKey] = false // free up this device so we can call another function on it
	devicesLockMutex.Unlock()
	Log(function + " - call just finished for " + socketKey)

	return
}

func functionStackProcessor() {
	function := "functionStackProcessor"
	defer func() {
		if r := recover(); r != nil {
			retMsg := fmt.Sprintf(function+" - bsdf354 unhandled panic: %v stack: %v restarting function stack processor", r, string(debug.Stack()[:]))
			AddToErrors("all", retMsg)
			Log("Restarting function stack processor")
			go functionStackProcessor()
		}
	}()

	displayCounter := 100
	var longerLastReportedTime time.Time
	longerReported := false

	for {
		now := time.Now()
		pos := 0
		stop := false
		called := false
		for len(functionStack) > 0 && !stop {
			// Log(fmt.Sprintf("checking for something to process on the function stack, len of stack is: %v", len(functionStack)))
			functionStackMutex.Lock()
			after := false
			if len(functionStackTimes) == 0 {
				after = false // nothing on the stack, so we should stop processing
			} else {
				after = now.After(functionStackTimes[pos]) // true if now is after this timestamp
			}
			functionStackMutex.Unlock()
			if after { // now is after the timestamp, so we have an entry that is ready for action
				// Log(fmt.Sprintf(function + " - item(s) are on functionStack: %v functionStackTimes: %v and time to run one",
				//	functionStack, functionStackTimes))
				peeked := functionStackPeek(pos)
				socketKey := peeked[2]

				devicesLockMutex.Lock()
				if val, ok := devicesLock[socketKey]; ok {
					_ = val // appease
					// have an entry, so no need to initialize it
				} else {
					devicesLock[socketKey] = false // start out unlocked
				}

				if devicesLock[socketKey] {
					// locked - can't access this device now
					// Log(fmt.Sprintf("incrementing pos because a device was locked.  pos: %v locked array: %v", pos, devicesLock))
					pos++
				} else {
					Log(fmt.Sprintf("Calling a function from position %v in the stack", pos))
					functionStackMutex.Lock()
					popped := functionStackPop(pos)
					functionStackMutex.Unlock()
					devicesLock[socketKey] = true // lock it before launching the thread to service this call

					// launch this call in a short-lived thread so we can process pending calls for another device
					// if there are any pending
					Log(fmt.Sprintf(function+" launching Call for "+socketKey+" %v", popped))
					go call(socketKey, popped[0], popped[2:])
					called = true
				}
				devicesLockMutex.Unlock()
				if pos >= len(functionStack) { // if we have run out of unlocked items on the function stack
					stop = true
				}

				// see if the function stack has too many entries
				checkFunctionStackSize()

				functionStackMutex.Lock()
				// see if items on our stack are getting too old
				for i, deadline := range functionStackTimes {
					if deadline.Add(time.Duration(RefreshDataPointsEvery) * time.Second).Before(time.Now()) {
						// we missed a full refresh cycle and we haven't reported this condition recently
						if !longerReported || longerLastReportedTime.Add(time.Duration(ReportOlderInterval)*time.Second).Before(time.Now()) {
							longerReported = true
							longerLastReportedTime = time.Now()
							errMsg := fmt.Sprintf(function+" -  7980klfad entry number %d on function stack is older than it should be %v compared to %v",
								i, deadline, time.Now())
							AddToErrors(socketKey, errMsg)
						}
					}
				}
				functionStackMutex.Unlock()
			} else { // the rest of the function stack isn't ready to run yet
				// Log("no more are ready for processing")
				stop = true
			}
		}
		displayCounter--
		if displayCounter == 0 {
			Log(function)
			displayCounter = 100
		}
		// Log(function+ " sleeping")
		if !called { // only sleep if we didn't call anything this time - we might have more to do right away
			time.Sleep(time.Second / 100)
		}
	}
	// execution won't reach this point (infinite loop above)
}

func functionStackAppend(toAppend []string) {
	functionStackMutex.Lock()
	functionStack = append(functionStack, toAppend)
	// Log(fmt.Sprintf("functionStackAppend - added an item, stack is: %v", functionStack))
	now := time.Now()
	deadline := now.Add(time.Duration(RefreshDataPointsEvery) * time.Second)
	// Log(fmt.Sprintf("APPENDING to function stack, deadline: %v now: %v", deadline, time.Now()))
	functionStackTimes = append(functionStackTimes, deadline)
	functionStackMutex.Unlock()
}

func functionStackPrepend(toPrepend []string) {
	functionStackMutex.Lock()
	functionStack = append([][]string{toPrepend}, functionStack...)
	now := time.Now()
	deadline := now.Add(0 * time.Second) // schedule it for ASAP
	// Log(fmt.Sprintf("PRE-PENDING to function stack, deadline: %v now: %v", deadline, time.Now()))
	functionStackTimes = append([]time.Time{deadline}, functionStackTimes...)
	functionStackMutex.Unlock()
}

func functionStackInsertAfterUpdates(toInsert []string) {
	// Add the operation after other update operations.  Used to preserve power on order even if
	//   the PUTs come in faster than the microservice can process them.

	now := time.Now()
	deadline := now.Add(0 * time.Second) // schedule it for ASAP
	functionStackMutex.Lock()
	// scan past any updates already on the function stack
	i := 0
	for i < len(functionStack) && functionStack[i][0] == "UpdateDoOnce" {
		i++
	}
	// Log(fmt.Sprintf("IIIIII: inserted at i = %d", i))
	if i == len(functionStack) { // nil slice or after last element
		functionStack = append(functionStack, toInsert)
		functionStackTimes = append(functionStackTimes, deadline)
	} else {
		functionStack = append(functionStack[:i+1], functionStack[i:]...)
		functionStack[i] = toInsert
		// Log(fmt.Sprintf("PRE-PENDING to function stack, deadline: %v now: %v", deadline, time.Now()))
		functionStackTimes = append(functionStackTimes[:i+1], functionStackTimes[i:]...)
		functionStackTimes[i] = deadline
	}
	functionStackMutex.Unlock()
}

func checkFunctionStackSize() {
	function := "checkFunctionStackSize"
	functionStackMutex.Lock()
	// see if our stack has gotten too big
	lenStack := len(functionStack)
	if lenStack > FunctionStackFull {
		errMsg := fmt.Sprintf(function+" - q32rf stack has %d entries which exceeds the full threshold of %d", lenStack, FunctionStackFull)
		AddToErrors("not associated with a particular socketKey", errMsg)
	}
	functionStackMutex.Unlock()
}

func functionStackPop(i int) []string { // Get a value in the stack at the specified index and pop and return it
	// NOTE: make sure you lock the functionStackMutex before calling this!
	toReturn := functionStack[i]
	functionStack = append(functionStack[0:i], functionStack[i+1:]...) // variadic function requires "..."" for some reason
	functionStackTimes = append(functionStackTimes[0:i], functionStackTimes[i+1:]...)

	return toReturn
}

func functionStackPeek(i int) []string { // Get a value in the stack at the specified index and return it without popping
	functionStackMutex.Lock()
	toReturn := functionStack[i]
	functionStackMutex.Unlock()

	return toReturn
}

func functionStackRemove(functionName string, endPoint string, socketKey string) bool {
	functionStackMutex.Lock()
	var i int = 0
	for _, s := range functionStack {
		if s[0] == functionName && s[1] == endPoint && s[2] == socketKey {
			functionStack = append(functionStack[:i], functionStack[i+1:]...)
			functionStackTimes = append(functionStackTimes[:i], functionStackTimes[i+1:]...)
			functionStackMutex.Unlock()
			return true
		}
		i++
	}
	functionStackMutex.Unlock()

	return false
}

func functionStackExist(functionName string, endPoint string, socketKey string) bool {
	// function := "functionStackExist"

	functionStackMutex.Lock()

	for _, s := range functionStack {
		if s[0] == functionName && s[1] == endPoint && s[2] == socketKey {
			functionStackMutex.Unlock()
			// Log(function + " - function " + functionName + " " + endPoint + " IS on " + socketKey + " stack")
			return true
		}
	}

	// Log(function + " - function NOT on stack")
	functionStackMutex.Unlock()
	return false
}

func CheckForEndPointInCache(socketKey string, endPoint string) bool {
	// function := "checkForEndPointInCache"

	deviceStatesMutex.Lock()
	defer deviceStatesMutex.Unlock()

	// Log(function + " - socketKey: " + socketKey + " endPoint: " + endPoint)
	// Log(fmt.Sprintf(function+" - deviceStates: %v", deviceStates))
	if _, ok := deviceStates[socketKey]; !ok {
		// Log(function + " - " + socketKey + " - first we hear of device")
		return false // wasn't in cache
	}
	if _, ok := deviceStates[socketKey][endPoint]; !ok {
		// Log(function + " - " + socketKey + " - first we hear of data point")
		return false
	}
	// Log(function + " - endpoint was in the cache")

	return true
}

func checkFunctionAppend(functionToCall string, endPoint string, socketKey string) bool {
	function := "CheckFunctionAppend"

	// log( function + " - checking if we should append the function call or not" )
	// log( fmt.Sprintf( "     functionStack: %v " + socketKey + " " + endPoint + " " + functionToCall, functionStack))
	if !functionStackExist(functionToCall, endPoint, socketKey) {
		// log(function + " - adding " + socketKey + " " + functionToCall + " " + endPoint + " because it wasn't on function stack already")
		return true
	} else if CheckFunctionAppendBehavior == "Don't add another instance" {
		// log(function + " - not adding " + socketKey + " " + functionToCall + " " + endPoint + " because a duplicate exists on the function stack already")
		return false
	} else if CheckFunctionAppendBehavior == "Remove older instance" {
		if functionToCall == "endPointRefresh" {
			Log(function + " - leaving older refresh duplicate on function stack and not adding a new one " + socketKey + " " + functionToCall + " " + endPoint + " that exists on the function stack already")
			return false
		} else {
			Log(function + " - replacing older duplicate on function stack " + socketKey + " " + functionToCall + " " + endPoint + " that exists on the function stack already")
			functionStackRemove(functionToCall, endPoint, socketKey)
			return true
		}
	} else if CheckFunctionAppendBehavior == "Add another instance" {
		Log(function + " - adding despite duplicate that is on function stack already")
		return true
	}
	AddToErrors(socketKey, function+" - adf23dsf found invalid value in checkFunctionAppendBehavior global variable: "+CheckFunctionAppendBehavior)
	return true
}

func establishSocketConnectionIfNeeded(socketKey string) bool {
	function := "establishSocketConnectionIfNeeded"

	if internalConnectionsMapExists(socketKey) {
		Log("establishSocketConnectionIfNeeded - " + socketKey + " - connection already in table")
		return true
	} else {
		Log("establishSocketConnectionIfNeeded - " + socketKey + " - connection needs to be established")
		socketAddress := socketKey
		if strings.Count(socketKey, "@") == 1 {
			socketAddress = strings.Split(socketKey, "@")[1]
		}

		if UseUDP {
			//We use ListenUDP instead of Dial in case a UDP device responds from a different port than the one we send to.
			//Seen with Sony cameras.
			listeningPort := ":" + fmt.Sprint(DefaultSocketPort)
			locaddr, err := net.ResolveUDPAddr("udp", listeningPort)
			connection, err := net.ListenUDP("udp", locaddr)
			if err != nil {
				AddToErrors(socketKey, function+" - "+socketKey+" - 423fsdaa error connecting: "+err.Error())
				return false
			} else if connection == nil {
				AddToErrors(socketKey, function+" - "+socketKey+" - dbtw33 no error connecting but connection is still nil")
				return false
			} else {
				Log("establishSocketConnectionIfNeeded - " + socketKey + " - connection successfully established")
				connectionsUDP[socketKey] = connection
			}
		} else { // TCP
			d := net.Dialer{Timeout: time.Duration(ConnectTimeout) * time.Second}
			connection, err := d.Dial("tcp", socketAddress)
			if err != nil {
				AddToErrors(socketKey, function+" - "+socketKey+" - q43fdsa error connecting: "+err.Error())
				return false
			} else if connection == nil {
				AddToErrors(socketKey, function+" - "+socketKey+" - bfdwdf3 no error connecting but connection is still nil")
				return false
			} else {
				Log("establishSocketConnectionIfNeeded - " + socketKey + " - connection successfully established")
				connectionsTCP[socketKey] = connection.(*net.TCPConn)

				// TCP success so make a reader
				global_reader[socketKey] = bufio.NewReader(connectionsTCP[socketKey])
			}
		}
	}

	deviceStatesMutex.Lock()
	if _, ok := deviceStates[socketKey]; !ok {
		deviceStates[socketKey] = make(map[string]string)
	}
	deviceStatesMutex.Unlock()

	return true
}

func CloseSocketConnection(socketKey string) {
	connectionsMutex.Lock()
	internalCloseSocketConnection(socketKey)
	connectionsMutex.Unlock()
}

func internalCloseSocketConnection(socketKey string) bool {
	if internalConnectionsMapExists(socketKey) {
		if UseUDP {
			connectionsUDP[socketKey].Close()
			delete(connectionsUDP, socketKey)
		} else {
			connectionsTCP[socketKey].Close()
			delete(connectionsTCP, socketKey)
		}
		// Log("internalCloseSocketConnection - " + socketKey + " - connection closed")
		return true
	}
	// we succeed no matter what - either it was gone already or we closed it or there never
	//     was a connection to close (e.g. Bravia)
	return true
}

func WriteLineToSocket(socketKey string, line string) bool {
	function := "WriteLineToSocket"
	connectionsMutex.Lock()
	defer connectionsMutex.Unlock()

	Log(function + " - writing: " + strings.Trim(line, "\r\n") + " to: " + socketKey)

	if !internalConnectionsMapExists(socketKey) {
		if !establishSocketConnectionIfNeeded(socketKey) {
			return addToErrorsAndReturn("all", function+" - "+socketKey+" - bsfd456ty connection not in table and could not establish it", false)
		}
	}

	if UseUDP {
		err := connectionsUDP[socketKey].SetWriteDeadline(time.Now().Add(time.Duration(WriteTimeout) * time.Second))
		if err != nil {
			return addToErrorsAndReturn(socketKey, function+" - "+socketKey+" - bdsf34432 can't set write timeout with: "+err.Error(), false)
		}
		socketAddress := strings.Split(socketKey, "@")[1]
		remoteaddr, err := net.ResolveUDPAddr("udp", socketAddress)
		if err != nil {
			AddToErrors(socketKey, function+" - "+socketKey+" - 95d4mc error creating remoteaddress: "+err.Error())
		}

		bytesWritten, err := connectionsUDP[socketKey].WriteToUDP([]byte(line), remoteaddr)
		bytesWritten = bytesWritten // appease

		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				AddToErrors(socketKey, function+" - "+socketKey+" - feqw234  write timeout: "+err.Error()+" closing socket connection")
			} else {
				AddToErrors(socketKey, function+" - "+socketKey+" - 324fsd write error: "+err.Error()+" closing socket connection")
			}
			internalCloseSocketConnection(socketKey)
			return false
		} else {
			Log("writeLineToSocket - " + socketKey + " - wrote " + strconv.Itoa(bytesWritten) + " bytes: " + line)
			return true
		}
	} else { // TCP
		err := connectionsTCP[socketKey].SetWriteDeadline(time.Now().Add(time.Duration(WriteTimeout) * time.Second))
		if err != nil {
			return addToErrorsAndReturn(socketKey, function+" - "+socketKey+" - can't set write timeout with: "+err.Error(), false)
		}

		bytesWritten, err := connectionsTCP[socketKey].Write([]byte(line))
		bytesWritten = bytesWritten // appease

		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				AddToErrors(socketKey, function+" - "+socketKey+" - 23qfasv write timeout: "+err.Error()+" closing socket connection")
			} else {
				AddToErrors(socketKey, function+" - "+socketKey+" - vfwa3 write error: "+err.Error()+" closing socket connection")
			}
			internalCloseSocketConnection(socketKey)
			return false
		} else {
			Log("writeLineToSocket - " + socketKey + " - wrote " + strconv.Itoa(bytesWritten) + " bytes: " + line)
			return true
		}
	}
}

func ReadLineFromSocket(socketKey string) string {
	function := "ReadLineFromSocket"
	Log("readLineFromSocket reading from: " + socketKey)

	// Some devices time out if we just do a read with a long (e.g. 5 seconds) timeout value.
	// So instead we try up to a certain number of times with a shorter timeout value.
	msg := ""
	tries := 0
	for len(msg) == 0 && tries < MaxReadTries {
		msg = tryReadLineFromSocket(socketKey)
		tries++
		if len(msg) == 0 && tries > ReadNoSleepTries { // failed multiple times, slow down our attempts
			// Log("readLineFromSocket sleeping")
			time.Sleep(time.Duration(ReadFailSleep) * time.Millisecond)
		}
	}

	Log(fmt.Sprintf(function+" - "+socketKey+" - 432qfe read : %s in %d tries", msg, tries))
	return msg
}

// This does the work of reading a line with a short timeout.  It can see if there is more pending input from the socket.
func tryReadLineFromSocket(socketKey string) string {
	function := "TryReadLineFromSocket"
	// Log("tryReadLineFromSocket reading from: " + socketKey)
	connectionsMutex.Lock()
	defer connectionsMutex.Unlock()

	if !internalConnectionsMapExists(socketKey) {
		if !establishSocketConnectionIfNeeded(socketKey) {
			AddToErrors("all", function+" - ljk54v "+socketKey+" - connection not in table and could not establish it")
			return ""
		}
	}

	var err error = nil
	data := make([]byte, 1024)
	var bytesRead = 0
	if !UseUDP { // TCP
		err = connectionsTCP[socketKey].SetReadDeadline(time.Now().Add(time.Duration(TryReadTimeout) * time.Millisecond))
		if err != nil {
			AddToErrors(socketKey, function+" - "+socketKey+" - n645ub can't set read timeout with: "+err.Error())
			return ""
		}
		data, err = global_reader[socketKey].ReadBytes(byte(GlobalDelimiter)) // Read up to the delimiter character
		//Log(fmt.Sprintf("RRRRRR  read: %v", string(data)))
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			Log("tryReadLineFromSocket - " + socketKey + " - nothing to read")
			// we expect to reach this case when there is no more from the socket or the device isn't ready yet,
			//      so timeout here is not an error

			bytesRead = 0
		} else if err == nil { // succeeded reading something
			bytesRead = bytes.Index(data, []byte(string(GlobalDelimiter)))
			//Log(fmt.Sprintf("RRRRR read %d bytes", bytesRead))
		} else {
			AddToErrors(socketKey, function+" - "+socketKey+" - error seeking GlobalDelimiter: "+err.Error())
			return ""
		}
	} else { // UDP
		err = connectionsUDP[socketKey].SetReadDeadline(time.Now().Add(time.Duration(TryReadTimeout) * time.Millisecond))
		if err != nil {
			AddToErrors(socketKey, function+" - "+socketKey+" - can't set read timeout with: "+err.Error())
			return ""
		}
		var addr *net.UDPAddr
		bytesRead, addr, err = connectionsUDP[socketKey].ReadFromUDP(data) // Read up to the delimiter character
		if err != nil {
			AddToErrors(socketKey, function+" - "+socketKey+" - 2131df Error reading: "+err.Error())
		} else {
			Log("Read from address: " + addr.String())
		}
		//if (err != nil) {
		//time.Sleep(100)  // sleep to give the remote UDP device a chance to get the packet back
		//bytesRead, err = connectionsUDP[socketKey].Read(data) // A UDP packet
		//}
	}
	// Log(fmt.Sprintf("RRRRRR2  read: %v", string(data)))
	ret := strings.TrimSpace(string(data))
	// Log("ret is: " + ret)
	Log(fmt.Sprintf("  "+function+" - "+socketKey+" - read %d bytes: %v", bytesRead, ret))

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			// we expect to reach this case when there is no more from the socket or the device isn't ready yet,
			//      so timeout here is not an error
			// Log("tryReadLineFromSocket - " + socketKey + " - nothing to read" )
			Log(fmt.Sprintf("  " + function + " - " + socketKey + " - TIMEOUT read : " + ret))
			if UseUDP {
				// Closing connection in case of timeout to let other UDP devices (e.g. VISCA cameras)
				//  connect with microservice.
				// Caused by AVer set power not sending a response back.
				internalCloseSocketConnection(socketKey)
			}
			// Telnet negotiations for the Biamp DSP do not have a terminator. The response is read in but the connection times out.
			// This allows us to see and respond to the negotiation.
			if UseTelnet {
				return ret
			} else {
				return ""
			}
		}
		if bytesRead == 0 {
			// we expect to reach this case when there is no more from the socket or the device isn't ready yet,
			//      so timeout here is not an error
			Log("tryReadLineFromSocket - " + socketKey + " - nothing to read")
			return ""
		} else {
			AddToErrors(socketKey, function+" - wrea354 "+socketKey+" - read error: "+err.Error()+" closing socket connection")
			internalCloseSocketConnection(socketKey)
		}
		Log("  " + function + " - q423fsa " + socketKey + " - read : " + ret)
		return ""
	}

	// Log("tryReadLineFromSocket - " + socketKey + " - read : " + string(data))
	Log("  " + function + " - vae345c " + socketKey + " - read : " + ret)
	return ret
}

// External function to check if a connection exists. Handles locking and unlocking the mutex.
func CheckConnectionsMapExists(socketKey string) bool {
	connectionsMutex.Lock()
	value := internalConnectionsMapExists(socketKey)
	connectionsMutex.Unlock()

	return value
}

// Utility function to avoid repeating weird golang syntax many times
// and to encapsulate UDP versus TCP connections differences
// Expected to be used inside the framework. Locking/unlocking happens outside this function.
func internalConnectionsMapExists(socketKey string) bool {
	if UseUDP {
		if _, ok := connectionsUDP[socketKey]; !ok {
			//Log("q3fasv UDP not connected")
			return false
		}
	} else { // TCP
		if _, ok := connectionsTCP[socketKey]; !ok {
			//Log("nhh45e456 TCP not connected")
			return false
		}
	}
	// Log("Socket Key has connection")
	return true // if we got here, there is already a map created
}

func Log(text string) {
	fmt.Println(getTimeStamp() + " - " + text)
}

func logError(text string) {
	fmt.Println(getTimeStamp() + " - \033[1;31m" + text + "\033[0m")
}

func getTimeStamp() string {
	loc, _ := time.LoadLocation("America/New_York")
	return time.Now().In(loc).Format("2006-01-02 15:04:05.000000")
}

func getSocketKey(context echo.Context) (string, error) {
	var address = context.Param("address")
	var port = DefaultSocketPort
	var username = ""
	var password = ""

	if strings.Count(address, "@") == 1 {
		// there are credentials in there
		credentials := strings.Split(address, "@")[0]
		address = strings.Split(address, "@")[1]
		if strings.Count(credentials, ":") == 1 {
			username = strings.Split(credentials, ":")[0]
			password = strings.Split(credentials, ":")[1]
		}
	}
	if strings.Count(address, ":") == 1 {
		// looks like a port was explicitely defined with the address
		var err error = nil
		_ = err // appease
		port, err = strconv.Atoi(strings.Split(address, ":")[1])
		address = strings.Split(address, ":")[0]
		if err != nil {
			return "", errors.New("port needs to be an integer")
		}
	}

	// "all" is a keyword used by getErrors, it intends to get errors for all devices
	if address == "all" {
		return address, nil
	}
	return username + ":" + password + "@" + address + ":" + strconv.Itoa(port), nil
}

func getErrors(context echo.Context) error {
	socketKey, err := getSocketKey(context)
	if err != nil {
		AddToErrors("all", "getErrors - 23sdsdw# couldn't get a socket key with error: "+err.Error())
		return context.JSON(http.StatusInternalServerError, "couldn't get a socket key with error: "+err.Error())
	}

	// Log("getErrors - " + socketKey)

	lastQueriedMutex.Lock()
	lastQueried[socketKey] = time.Now()
	lastQueriedMutex.Unlock()

	httpResponseCode := http.StatusOK
	errs, gotErrors := deviceErrors[socketKey]
	if gotErrors {
		if len(errs) > 0 {
			httpResponseCode = http.StatusInternalServerError
			deviceErrors[socketKey] = make(map[string]string)
		} else {
			gotErrors = false
		}
	}

	// Log( "getErrors - " + socketKey + " - " + strings.Replace(errs, "\n", ", ", -1) )
	return context.JSON(httpResponseCode, errs)
}

// Prints the error and adds it the deviceErrors map
func AddToErrors(socketKey string, errorMessage string) {
	deviceErrorsMutex.Lock()
	Log("addToErrors - logging an error: " + errorMessage)
	logError(errorMessage)
	if _, ok := deviceErrors[socketKey]; !ok {
		deviceErrors[socketKey] = make(map[string]string)
	}
	deviceErrors[socketKey][getTimeStamp()] = errorMessage
	deviceErrorsMutex.Unlock()
}

func addToErrorsAndReturn(socketKey string, errorMessage string, toReturn bool) bool {
	AddToErrors(socketKey, errorMessage)

	return toReturn
}

func handleGet(context echo.Context) error {
	function := "handleGet"

	var valueMap interface{}
	setting := context.Param("setting")
	arg1 := context.Param("arg1") // If these don't exist, echo nicely returns an empty string
	arg2 := context.Param("arg2")
	endPoint := setting + arg1 + arg2
	retMsg := "uninitialized"
	retCode := http.StatusOK

	socketKey, err := getSocketKey(context)
	defer func() {
		if r := recover(); r != nil {
			retMsg := fmt.Sprintf(function+" - ndh7456 unhandled panic: %v stack: %v", r, string(debug.Stack()[:]))
			AddToErrors(socketKey, retMsg)
		}
	}()
	Log("GET " + function + " - " + socketKey + " handling " + endPoint + " get " + arg1 + " " + arg2)
	if err != nil {
		AddToErrors("all", function+" - a%&dssd couldn't get a socket key with error: "+err.Error())
		return context.JSON(http.StatusInternalServerError, "couldn't get a socket key with error: "+err.Error())
	}

	lastQueriedMutex.Lock()
	lastQueried[socketKey] = time.Now() // extend the refresh cycle
	lastQueriedMutex.Unlock()

	//  If we haven't already gotten a value for this end point we need to respond that it is unknown (HTTP 204)
	if !CheckForEndPointInCache(socketKey, endPoint) {
		// No value in the cache for this endpoint, report HTTP 204 error
		retMsg = function + " - f432fc# haven't gotten a device value yet for " + socketKey + " " + endPoint
		retCode = http.StatusNoContent
	} else {
		// Use the value in the cache
		deviceStatesMutex.Lock()
		data := []byte(deviceStates[socketKey][endPoint])
		retMsg = string(data)
		err = json.Unmarshal(data, &valueMap)
		// Log(fmt.Sprintf(function + " UUUU data is: %v, valueMap is:%v", string(data), valueMap))
		if err != nil {
			retMsg = function + " - vsb654 error unmarshaling value from the deviceStates cache (probably caused by bad formatting in device specific get function) u98hjlnkl: " + deviceStates[socketKey][endPoint] + " " + socketKey + " " + endPoint
			AddToErrors(socketKey, retMsg)
			delete(deviceStates[socketKey], endPoint) // get the offending value out of the cache so we have a chance to recover
			retCode = http.StatusNotFound
		}
		deviceStatesMutex.Unlock()
	}

	// Make sure we will refresh this value in the cache regardless of whether or not this was a cache hit.
	functionStackFunction := "endPointRefresh"
	// Note that the repetition of endPoint in the parameters is necessary - each value is used in a different layer
	if retCode != http.StatusOK {
		// Cache miss - need to get a refresh ASAP, but after any pending updates
		if arg2 != "" {
			functionStackInsertAfterUpdates([]string{functionStackFunction, endPoint, socketKey, setting, endPoint, arg1, arg2})
		} else if arg1 != "" {
			functionStackInsertAfterUpdates([]string{functionStackFunction, endPoint, socketKey, setting, endPoint, arg1})
		} else {
			functionStackInsertAfterUpdates([]string{functionStackFunction, endPoint, socketKey, endPoint, setting})
		}
	} else {
		// Cache hit - schedule a normal refresh for later
		if checkFunctionAppend(functionStackFunction, endPoint, socketKey) {
			if arg2 != "" {
				functionStackAppend([]string{functionStackFunction, endPoint, socketKey, setting, endPoint, arg1, arg2})
			} else if arg1 != "" {
				functionStackAppend([]string{functionStackFunction, endPoint, socketKey, setting, endPoint, arg1})
			} else {
				functionStackAppend([]string{functionStackFunction, endPoint, socketKey, endPoint, setting})
			}
		}
	}

	Log(fmt.Sprintf("GET "+function+" - Returning: %v : %v for: "+socketKey+" "+endPoint, retCode, retMsg))
	return context.JSON(retCode, valueMap)
}

var callDevice = true // Hack to get around foibles of golang panic processing:
// We need side effects (e.g. refresh scheduling) to happen even though we
// panicked in device processing, we still need to finish processing in this function
// so we call again and use a global to prevent device processing the second time.
// Ugly, but it works.

func endPointRefresh(arguments []string) bool {
	function := "endPointRefresh"
	refreshError := false

	dataUpdated := false
	socketKey := arguments[0]
	setting := arguments[1]
	endPoint := arguments[2]

	var arg1 string = ""
	if len(arguments) > 3 {
		arg1 = arguments[3]
	}
	var arg2 string = ""
	if len(arguments) > 4 {
		arg2 = arguments[4]
	}

	Log(fmt.Sprintf(function+" - arguments are: %v", arguments))

	// priming data structure
	deviceStatesMutex.Lock()
	if _, ok := deviceStates[socketKey]; !ok {
		deviceStates[socketKey] = make(map[string]string)
		// Log(fmt.Sprintf("created deviceStates entry for %s: deviceStates: %v", socketKey, deviceStates))
	}
	if _, ok := deviceStates[socketKey][endPoint]; !ok {
		deviceStates[socketKey][endPoint] = `"unknown"` // back ticks make this legal JSON for unmarshaling purposes
		// Log(fmt.Sprintf("created endpoint: %s deviceStates: %v", endPoint, deviceStates))
	}
	deviceStatesMutex.Unlock()

	deviceMutex.Lock()
	defer func() {
		if r := recover(); r != nil {
			retMsg := fmt.Sprintf(function+" - vsdser546 unhandled panic: %v stack: %v", r, string(debug.Stack()[:]))
			AddToErrors(socketKey, retMsg)
			callDevice = false // make the recursive call noted above
			deviceMutex.Unlock()
			dataUpdated = endPointRefresh(arguments)
			callDevice = true // let the next invocation try the device again
		}
	}()
	var resp string = "unknown"
	var err error = nil
	if callDevice {
		// Log("Accessing the device!!")
		resp, err = doDeviceSpecificGet(socketKey, setting, arg1, arg2)
		// Log("response = " + resp)
	}
	deviceMutex.Unlock()

	if err == nil {
		// Log(fmt.Sprintf(function + " - putting %v into deviceStates[%v][%v]", resp, socketKey, endPoint))
		deviceStatesMutex.Lock()
		deviceStates[socketKey][endPoint] = resp // store value in the cache
		deviceStatesMutex.Unlock()
		dataUpdated = true
	} else {
		AddToErrors(socketKey, function+" - "+socketKey+" - nhtg4653w couldn't get the setting: "+setting)
		refreshError = true
	}

	if !KeepAlive {
		CloseSocketConnection(socketKey)
	}

	lastQueriedMutex.Lock()
	// Log(fmt.Sprintf(function + " - NOW: %v LAST QUERIED: %v", time.Now(), lastQueried[socketKey]))
	// renew the refresh cycle if appropriate
	if !CheckForEndPointInCache(socketKey, endPoint) ||
		time.Duration(time.Now().Sub(lastQueried[socketKey])) < time.Duration(time.Duration(KeepRefreshingFor)*time.Second) {
		Log(function + " - " + socketKey + " - ef3241 device freshly in use, we'll update it again later")
		if !refreshError && checkFunctionAppend(function, endPoint, socketKey) {
			functionStackAppend(append([]string{function, endPoint}, arguments...))
		}
	} else {
		Log(function + " - " + socketKey + " " + endPoint + " - device not freshly in use, no need to keep refreshing it")
		deviceStatesMutex.Lock()
		delete(deviceStates[socketKey], endPoint)
		deviceStatesMutex.Unlock()
	}
	lastQueriedMutex.Unlock()

	return dataUpdated
}

func handleUpdate(context echo.Context) error {
	function := "handleUpdate"

	setting := context.Param("setting")
	arg1 := context.Param("arg1")
	arg2 := context.Param("arg2")
	endPoint := setting + arg1 + arg2

	socketKey, err := getSocketKey(context)
	defer func() {
		if r := recover(); r != nil {
			retMsg := fmt.Sprintf(function+" - lnnui3q23 unhandled panic: %v stack: %v", r, string(debug.Stack()[:]))
			AddToErrors(socketKey, retMsg)
		}
	}()

	Log("PUT " + function + " - " + socketKey + " handling " + endPoint + " update " + arg1 + " " + arg2)
	if err != nil {
		AddToErrors("all", function+" - wrg765 couldn't get a socket key with error: "+err.Error())
		return context.JSON(http.StatusInternalServerError, "couldn't get a socket key with error: "+err.Error())
	}

	// Extend the refresh cycle
	lastQueriedMutex.Lock()
	lastQueried[socketKey] = time.Now()
	// Log("PUT " + fmt.Sprintf("time.Now(): %v lastQueried[socketKey]: %v", time.Now(), lastQueried[socketKey]))
	lastQueriedMutex.Unlock()

	buf := new(bytes.Buffer)
	buf.ReadFrom(context.Request().Body)
	data := []byte(buf.String())
	var valueMap interface{}
	Log(function + " - vawe352 value from PUT body is: " + string(data))
	if len(data) > 0 {
		err = json.Unmarshal(data, &valueMap)
		if err != nil {
			errMsg := function + " - error unmarshaling value from PUT body  y87hkl: " + string(data)
			AddToErrors(socketKey, function+errMsg)
			return context.JSON(http.StatusNotFound, errMsg)
		}
	}
	var newStateBytes []byte
	newStateBytes, err = json.Marshal(valueMap)
	if err != nil { // shouldn't ever happen because we successfully unmarshaled it
		AddToErrors(socketKey, function+" - mfh45gfh couldn't marshal JSON to put into the cache")
		return context.JSON(http.StatusNotFound, "error marshaling value to put into the cache  43qfsdsfd;")
	}
	newState := string(newStateBytes)
	if !strings.Contains(newState, `"`) {
		// this one is a native JSON type, but we store everything in the cache as quoted strings, so add quotes
		newState = `"` + newState + `"`
	}

	deviceStatesMutex.Lock()
	// updating data structure
	if _, ok := deviceStates[socketKey]; !ok {
		deviceStates[socketKey] = make(map[string]string)
	}
	// Add quotes to the value to mimic what the device specific gets do (maybe add them elsewhere?) this is messy
	//newState = `"` + newState + `"`
	// Log(function + " - storing " + newState + " in deviceStates " + socketKey + " " + endPoint)
	deviceStates[socketKey][endPoint] = newState
	doRelatedActions(socketKey, setting, arg1+arg2)
	deviceStatesMutex.Unlock()

	// Put the update on the function stack to happen ASAP but without our REST client having to wait
	//    and after any other pending updates (especially after power on which can generate
	//    "unavailable time" type problems for other updates)
	functionStackFunction := "UpdateDoOnce"
	if checkFunctionAppend(functionStackFunction, endPoint, socketKey) {
		if arg2 != "" {
			functionStackInsertAfterUpdates([]string{functionStackFunction, endPoint, socketKey, setting, arg1, arg2, newState})
		} else if arg1 != "" {
			functionStackInsertAfterUpdates([]string{functionStackFunction, endPoint, socketKey, setting, arg1, newState})
		} else {
			functionStackInsertAfterUpdates([]string{functionStackFunction, endPoint, socketKey, setting, newState})
		}
	}

	Log("PUT " + function + " - added to function stack: Set " + endPoint + " " + socketKey + " " + setting + ", " + arg1 + ", " + arg2 + " to " + newState)
	return context.JSON(http.StatusOK, "ok")
}

func doRelatedActions(socketKey string, setting string, params string) {
	function := "DoRelatedActions"
	for i := range RelatedActions {
		relatedEndPoint := RelatedActions[i]["endPoint"]
		relatedValue := RelatedActions[i]["value"]
		if i == setting { // have an operation that needs a related action
			Log(function + " - setting cache value" + i + " " + relatedEndPoint + params + " " + relatedValue)
			deviceStates[socketKey][relatedEndPoint+params] = relatedValue
		}
	}
}

// Can be called externally to get the cached value for a device endpoint
func GetDeviceStateEndpoint(socketKey string, endpoint string) string {
	deviceStatesMutex.Lock()
	value := deviceStates[socketKey][endpoint]
	deviceStatesMutex.Unlock()

	return value
}

// Can be called externally to set the cached value for a device endpoint
func SetDeviceStateEndpoint(socketKey string, endpoint string, value string) {
	deviceStatesMutex.Lock()
	deviceStates[socketKey][endpoint] = value
	deviceStatesMutex.Unlock()
}

func updateDoOnce(arguments []string) bool {
	function := "UpdateDoOnce"

	socketKey := arguments[0]
	Log(fmt.Sprintf(function+" - got arguments: %v", arguments))
	setting := arguments[1]
	var arg1 string = ""
	if len(arguments) > 2 {
		arg1 = arguments[2]
	}
	var arg2 string = ""
	if len(arguments) > 3 {
		arg2 = arguments[3]
	}
	var arg3 string = ""
	if len(arguments) > 4 {
		arg3 = arguments[4]
	}

	// Log(function + " - " + socketKey + " - setting: " + setting + "to: " + arg1 + " " + arg2 + " " + arg3)

	deviceMutex.Lock()
	defer func() {
		if r := recover(); r != nil {
			retMsg := fmt.Sprintf(function+" - bf34fsd unhandled panic: %v stack: %v", r, string(debug.Stack()[:]))
			AddToErrors(socketKey, retMsg)
			deviceMutex.Unlock()
		}
	}()
	resp, err := doDeviceSpecificSet(socketKey, setting, arg1, arg2, arg3)
	deviceMutex.Unlock()
	resp = resp // appease

	/*if !KeepAlive {
		CloseSocketConnection(socketKey)
	}*/

	if err != nil {
		if !KeepAlive {
			CloseSocketConnection(socketKey)
		}
		return false
	}

	// we already put the set value in the cache before this function was called, so no need to do that now
	return true
}

// Used for the Bravia microservice
// This function started from a tutorial: https://www.soberkoder.com/consume-rest-api-go/
func DoPost(socketKey string, theURL string, jsonReq string) (string, error) {
	function := "DoPost"
	postURL := "http://" + socketKey + "/" + theURL
	Log("======> " + function + " - doing POST to: " + postURL + " with contents: " + jsonReq)
	jsonReqBytes := []byte(jsonReq)
	password := ""
	apiKey := ""

	if strings.Count(socketKey, "@") == 1 {
		// There are credentials in there
		credentials := strings.Split(socketKey, "@")[0]
		if strings.Count(credentials, ":") == 1 {
			password = strings.Split(credentials, ":")[1]
		}
	}

	req, err := http.NewRequest(http.MethodPost, postURL,
		bytes.NewBuffer(jsonReqBytes))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	// An apiKey can be passed through as a password in the URL for the microservice.
	// If none is provided, 1234 is used by default.
	if password != "" {
		apiKey = password
	} else {
		apiKey = "1234"
	}
	// Log("APIKEY: " + apiKey)
	req.Header.Set("x-auth-psk", apiKey)
	client := &http.Client{
		Timeout: 2 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		errMsg := fmt.Sprintf(function+" - 423dfsaknnl HTTP client.DO error: %v", err)
		Log(errMsg)
		AddToErrors(socketKey, errMsg)
		return errMsg, err
	}

	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		errMsg := fmt.Sprintf(function+" - 32rqfsada HTTP ioutil.ReadAll error: %v", err)
		Log(errMsg)
		AddToErrors(socketKey, errMsg)
		return errMsg, err
	}

	// Convert response body to string
	bodyString := string(bodyBytes)
	Log("<====== " + function + " - got bodyString: " + bodyString)

	return bodyString, nil
}
