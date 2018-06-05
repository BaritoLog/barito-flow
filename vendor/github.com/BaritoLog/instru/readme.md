# Instru

Simple go instrumentation library for flexible push/pull based data strategy

## Instrumenting your code

#### Evaluation time

```go
func myFunction()  {
  eval := instru.Evaluate("myFunction")
  defer eval.Done()
  
  
  d, _ := time.Duration("2s")
  time.Sleep(d)
}
```

Sample of Evaluation metric (in nano seconds) 
```js
{
  "evaluations": {
    "myFunction": {
      "count": 12,
      "avg": 5000,
      "max": 10000,
      "min": 1000,
      "recent": 1000
    }
  }
}
```

#### Counter

```go
func oddOrEven()  {
	if rand.Int31()%2 == 0 {
		instru.Counter("odd_or_even").Event("odd")
	} else {
		instru.Counter("odd_or_even").Event("even")
	}
}
```

Sample of Counter metric 
```js
{
  "counters": {
    "odd_or_even": {
      "total": 21,
      "events": {
        "odd": 9,
        "even": 12
      }
    }
  }
}
```

## Pull the Instrumentation Metric 

#### Using RESTful API

Enable RESTful Server
```go
func main()  {
  instru.ExposeWithRestful(":8998")
}
```

Now you can check the metric via curl
```sh
curl http://localhost:8998
```

Sample of Instrumentation metric
```js
{
  "evaluations": {
    "myFunc": {
      "count": 12,
      "avg": 5000,
      "max": 10000,
      "min": 1000,
      "recent": 1000
    }
  },
  "counters": {
    "oddOrEvn": {
      "total": 21,
      "events": {
        "odd": 9,
        "even": 12
      }
    }
  }
}
```

#### Create Custom Exposer

```go
type CustomExposer struct{
}

// Expose is required
func (e *CustomExposer)Expose(instr intru.Instrumenation) (err error)  {
  // TODO: expose implementation
  return
}

// Stop is required 
func (e *CustomExposer) Stop()  {
  // TODO: stop implementation
}
```
```go
func main()  {
  exposer := &CustomExposer{}
  instru.Expose(exposer)
}
```

## Push the Instrumentation Metric 

#### Web Callback

Set a web callback
```go
func main()  {
  interval, _ := time.ParseDuration("5m")
  SetWebCallback(interval, "http://my-server:3042")
}
```

#### Create Custom Callback

```go
type MyCallback struct{
}

func (c *MyCallback) OnCallback(instr Instrumentation) (err error) {
  // TODO: OnCallback implementation
  return
}
```
```go
func main()  {
  interval, _ := time.ParseDuration("5m")
  instru.SetCallback(interval, &MyCallback{})  
}
```
