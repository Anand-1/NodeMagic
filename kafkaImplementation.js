require('tsx/cjs');

const implementation = require('./kafkaImplementation.ts');

module.exports = implementation;

if (require.main === module) {
	implementation.runCli().catch(error => {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	});
}
