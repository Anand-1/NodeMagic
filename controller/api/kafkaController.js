const { publishPost } = require('../../kafkaImplementation');

exports.showTriggerForm = (req, res) => {
	res.render('kafka', { status: null, message: null, submittedText: null });
};

exports.triggerKafka = async (req, res) => {
	try {
		const post = await publishPost(req.body.texts);
		return res.render('kafka', {
			status: 'success',
			message: `Post ${post.id} was published to Kafka.`,
			submittedText: post.text
		});
	} catch (error) {
		const isInvalidPost = error instanceof TypeError;
		if (!isInvalidPost) {
			console.error('Kafka publish failed:', error.message);
		}

		return res.status(isInvalidPost ? 400 : 503).render('kafka', {
			status: 'error',
			submittedText: null,
			message: isInvalidPost
				? error.message
				: 'Kafka is unavailable. Check the broker settings and try again.'
		});
	}
};