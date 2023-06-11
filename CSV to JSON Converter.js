const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

// Check if directory path is provided
const directoryPath = process.argv[2];
if (!directoryPath) {
    console.error('Error: Directory path not provided.');
    process.exit(1);
}

// Create the 'converted' directory if it doesn't exist
const convertedPath = path.join(__dirname, 'converted');
if (!fs.existsSync(convertedPath)) {
    fs.mkdirSync(convertedPath);
}

// Function to convert CSV to JSON
function convertToJSON(csvFilePath, callback) {
    const results = [];
    fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', (data) => results.push(data))
        .on('end', () => callback(null, results))
        .on('error', (error) => callback(error));
}

// Master process
if (cluster.isMaster) {
    console.log(`Master process ID: ${process.pid}`);

    // Read all CSV files in the directory
    fs.readdir(directoryPath, (err, files) => {
        if (err) {
            console.error('Error reading directory:', err);
            process.exit(1);
        }

        const csvFiles = files.filter((file) => path.extname(file).toLowerCase() === '.csv');

        const fileChunks = [];
        const chunkSize = Math.ceil(csvFiles.length / numCPUs);
        for (let i = 0; i < csvFiles.length; i += chunkSize) {
            fileChunks.push(csvFiles.slice(i, i + chunkSize));
        }

        // Fork worker processes
        fileChunks.forEach((chunk) => {
            const worker = cluster.fork();
            worker.send(chunk);
        });

        let totalCount = 0;
        let startTime = new Date().getTime();

        // Listen for messages from worker processes
        Object.values(cluster.workers).forEach((worker) => {
            worker.on('message', (message) => {
                if (message.type === 'count') {
                    totalCount += message.count;
                }
                if (message.type === 'completed') {
                    console.log(`Worker process ${worker.process.pid} completed.`);
                }
            });
        });

        // Listen for worker process exit
        cluster.on('exit', (worker, code, signal) => {
            console.log(`Worker process ${worker.process.pid} exited with code ${code}.`);
        });

        // Wait for all worker processes to finish
        cluster.on('exit', () => {
            let endTime = new Date().getTime();
            let duration = endTime - startTime;
            console.log(`\nTotal records: ${totalCount}`);
            console.log(`Parsing duration: ${duration}ms`);
            process.exit(0);
        });
    });
} else {
    // Worker process
    process.on('message', (csvFiles) => {
        csvFiles.forEach((file) => {
            const csvFilePath = path.join(directoryPath, file);
            const jsonFilePath = path.join(convertedPath, path.parse(file).name + '.json');

            convertToJSON(csvFilePath, (error, results) => {
                if (error) {
                    console.error(`Error parsing ${file}:`, error);
