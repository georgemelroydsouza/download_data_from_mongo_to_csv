
const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
const util = require('util');
const path = require('path');
const Long = require('mongodb').Long;
const csv = require('csv-parser');
const filePath = path.join(__dirname, "../user.csv");
const outputDir = '/tmp/output/';

//Connection URL
const uri = "mongodb://mongodb01:27017/orders?replicaSet=userorder";
let quitting = false;
let db;
let userData = new Array;
const iterationLimit = 8000;

function cleanExit() {
    // Closing the connection
    console.log('Closing the connection');
    db.close();
}

process.on('SIGINT', function() {
	console.log("Caught interrupt signal");
    quitting = true;
});
    
// Initialize connection once
MongoClient.connect(uri, function(err, database) {
    if(err) throw err;

    (async () => {
        db = database;
        
        // loop through the users CSV file and load user data into an array
        await loadCSVDataIntoArray();

        // loop through the array and process each row
        for (const row in userData) {
            console.log('Processing User: ', userData[row].id, ' REF: ', userData[row].ref);
            await processRow(userData[row].id, userData[row].ref);
        }
        
        cleanExit();
    })(); 
});

// function to fetch the csv data
async function loadCSVDataIntoArray() {
    return new Promise(function (resolve, reject) {
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => {
                
                if (isValidCSVRow(row) == true) {
                    userData.push(row);
                }
            })
            .on('end', () => {
                resolve(userData);
            });
    });
}

function isValidCSVRow(row) {
    if (row.id === "") {
        return false;
    }
    if (row.ref === "") {
        return false;
    } 
    if (typeof row.id === "undefined") {
        return false
    }
    if (typeof row.ref === "undefined") {
        return false
    }
    return true;
}

function processRow(userId, nui) {
    return new Promise(function (resolve, reject) {
        if (quitting == true) {
            resolve(null);
        }
        try {
            
            (async () => {
                const total = await dbUserrLocationCount(userId);
                
                if (total == 0) {
                    resolve(null);
                }
                
                console.log('Total rows to copy ' + total);
                
                const fileName = userId + "_output.csv";
                
                await checkFileExistsAndDelete(fileName);
                await createNewFile(fileName);
                let startRecord = 0;
                while (startRecord <= total) {
                    
                    console.log('Copying ' + iterationLimit + ' lines, starting at ' + startRecord);
                    
                    await dbFetchAndUpdateOrdersToFile(fileName, userId, ref, startRecord);
                    
                    startRecord += iterationLimit;
                }
                
                resolve(null);
            })();
            
        } catch (err) {
            reject(err);
        }
    });
}

function checkFileExistsAndDelete(fileName) {
    return new Promise(function (resolve, reject) {
        if (quitting == true) {
            resolve(null);
        }
        
        if (fs.existsSync(outputDir + fileName)) {
            fs.unlink(outputDir + fileName, function (err) {
                    if (err) {
                        console.log(err);
                    }
                resolve(null);
                });
        }
        else {
            resolve(null);    
        }
    });
}

function createNewFile(fileName) {
    return new Promise(function (resolve, reject) {
        if (quitting == true) {
            resolve(null);
        }

        fs.appendFile(outputDir + fileName, 'id,Fecha,NUI,Latitud,Longitud,Nivel de Bateria,Estado,Tipo de Ubicación,Velocidad,Exactitud,Estado Correa,Intensidad de la Señal,MCC,MNC,CELL ID,Red\r\n', function (err) {
            
            resolve(null);
            
            });
    });
}

function dbUserrLocationCount(userId) {
    return new Promise(function (resolve, reject) {
        if (quitting == true) {
            resolve(null);
        }
        db.collection('orders').count({ 'user_id': parseInt(userId) }, function (err, count) {
            if (quitting == false) {
                resolve(count);
            }
        });
    });
}

function dbFetchAndUpdateOrdersToFile(fileName, userId, nui, start) {
    return new Promise(function (resolve, reject) {
        if (quitting == true) {
            resolve(null);
        }
        let query = {
            'user_id': parseInt(userId)
        }
        db.collection('orders')
            .find(query).sort({ 'order_time': 1 })
            .limit(iterationLimit)
            .skip(parseInt(start))
            .toArray(function (err, results) {
            if (quitting == false) {

                (async () => {
                    for (const loop in results) {
                        
                        const record = results[loop];
                        
                        
                        let data = {
                            'id': record.id,
                            'order+_time': record.order_time,
                            'ref': ref
                        }
                        
                        await appendOrdersToFile(fileName, data);
                        
                    }
                })();
                
                resolve(null);
            }
        });
    });
}

function appendOrdersToFile(fileName, record) {
    return new Promise(function (resolve, reject) {
        
        let res = [];
        Object.keys(record).forEach(function(key) {
            res.push(record[key]);
        });
        
        fs.appendFile(outputDir + fileName, res.join(',') + '\r\n', function (err) {
                    
            resolve(null);
            
            });
        
    });
}
