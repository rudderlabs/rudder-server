//This is a template script created for some testing
//This is not the actual transformer server
const cluster = require('cluster');
const http = require('http');
const numCPUs = require('os').cpus().length;
//const numCPUs = 1;
const url = require("url");

function start(port){
    if(!port){
        port = 9191;
    }

    if (cluster.isMaster) {
        console.log(`Master ${process.pid} is running`);

    // Fork workers.
        for (let i = 0; i < numCPUs; i++) {
            cluster.fork();
        }

        cluster.on('exit', (worker, code, signal) => {
            console.log(`worker ${worker.process.pid} died`);
        });
    } else {
        //Main server body
        http.createServer(function (request,response){
            var pathname = url.parse(request.url).pathname;
            
            //Adding logic for a call that will invalidate cache
            //for particular module in order that next require call for
            //that module will reload the same
            if (request.method == 'POST') {
                var body = '';
                var respBody = '';	

                request.on('data', function (data) {
                    body += data;

		    console.log("Payload");
                    // Too much POST data, kill the connection!
                    // 1e6 === 1 * Math.pow(10, 6) === 1 * 1000000 ~~~ 1MB
			// /*
                    if (body.length > 1e8) {
			console.log("Huge payload");
                        request.connection.destroy();
		    }
                });

                request.on('end', async function () {
                    try {	//need to send 400 error for malformed JSON

                        response.statusCode = 200;
			console.log(body);
                        response.end(body);

                    } catch (se) {
                
                        response.statusCode = 500;	//500 for other errors
                        response.statusMessage = se.message;
                        console.log(se.stack);
                        response.end()	
                    }
                });
            }
        }).listen(port);
        console.log(`Worker ${process.pid} started`);
    }
    console.log("echoServer: started")
}

start(9191);
