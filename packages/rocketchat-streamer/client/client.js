/* globals DDPCommon, EV */
/* eslint-disable new-cap */

const NonEmptyString = Match.Where(function (x) {
	check(x, String);
	return x.length > 0;
});

class StreamerCentral extends EV {
	constructor() {
		super();

		this.instances = {};
		this.ddpConnections = {};		// since each Streamer instance can provide its own ddp connection, store them by streamer name

	}

	setupDdpConnection(name, server, ddpConnection) {
		// make sure we only setup event listeners for each ddp connection once
		if (ddpConnection.hasMeteorStreamerEventListeners) {
			return;
		}
		ddpConnection._stream.on('message', (raw_msg) => {
			const msg = DDPCommon.parseDDP(raw_msg);
			if (msg && msg.msg === 'changed' && msg.collection && msg.fields && msg.fields.eventName && msg.fields.args) {
				msg.fields.args.unshift(msg.fields.eventName);
				msg.fields.args.unshift(msg.collection +'-'+ server);
				this.emit.apply(this, msg.fields.args);
			}
		});
		// store ddp connection
		this.storeDdpConnection(name, server, ddpConnection);

	}

	storeDdpConnection(name, server, ddpConnection) {
		// mark the connection as setup for Streamer, and store it
		ddpConnection.hasMeteorStreamerEventListeners = true;
		this.ddpConnections[name + server] = ddpConnection;
	}
}

Meteor.StreamerCentral = new StreamerCentral;

Meteor.Streamer = class Streamer extends EV {
	constructor(name, {useCollection = false, server = '', ddpConnection = Meteor.connection } = {}) {
    server = server || '';
    if (Meteor.StreamerCentral.instances[name + server]) {
			console.warn('Streamer instance already exists:', name + server);
			return Meteor.StreamerCentral.instances[name + server];
		}
		Meteor.StreamerCentral.setupDdpConnection(name, server, ddpConnection);

		super();

		this.ddpConnection = ddpConnection || Meteor.connection;

		Meteor.StreamerCentral.instances[name + server] = this;

		this.name = name;
		this.server = server;
		this.useCollection = useCollection;
		this.subscriptions = {};

		Meteor.StreamerCentral.on(this.subscriptionName +'-'+ this.server, (eventName, ...args) => {
			if (this.subscriptions[eventName]) {
				this.subscriptions[eventName].lastMessage = args;
				super.emit.call(this, eventName, ...args);
			}
		});

		this.ddpConnection._stream.on('reset', () => {
			super.emit.call(this, '__reconnect__');
		});
	}

	get name() {
		return this._name;
	}

	set name(name) {
		check(name, String);
		this._name = name;
	}

	get subscriptionName() {
		return `stream-${this.name}`;
	}

	get useCollection() {
		return this._useCollection;
	}

	set useCollection(useCollection) {
		check(useCollection, Boolean);
		this._useCollection = useCollection;
	}

	stop(eventName) {
		if (this.subscriptions[eventName] && this.subscriptions[eventName].subscription) {
			this.subscriptions[eventName].subscription.stop();
		}
		this.unsubscribe(eventName);
	}

	stopAll() {
		for (let eventName in this.subscriptions) {
			if (this.subscriptions.hasOwnProperty(eventName)) {
				this.stop(eventName);
			}
		}
	}

	unsubscribe(eventName) {
		this.removeAllListeners(eventName);
		delete this.subscriptions[eventName];
	}

	subscribe(eventName) {
		let subscribe;
		Tracker.nonreactive(() => {
			subscribe = this.ddpConnection.subscribe(this.subscriptionName, eventName, this.useCollection, {
				onStop: () => {
					this.unsubscribe(eventName);
				}
			});
		});
		return subscribe;
	}

	onReconnect(fn) {
		if (typeof fn === 'function') {
			super.on('__reconnect__', fn);
		}
	}

	getLastMessageFromEvent(eventName) {
		const subscription = this.subscriptions[eventName];
		if (subscription && subscription.lastMessage) {
			return subscription.lastMessage;
		}
	}

	once(eventName, callback) {
		check(eventName, NonEmptyString);
		check(callback, Function);

		if (!this.subscriptions[eventName]) {
			this.subscriptions[eventName] = {
				subscription: this.subscribe(eventName)
			};
		}

		super.once(eventName, callback);
	}

	on(eventName, callback) {
		check(eventName, NonEmptyString);
		check(callback, Function);

		if (!this.subscriptions[eventName]) {
			this.subscriptions[eventName] = {
				subscription: this.subscribe(eventName)
			};
		}

		super.on(eventName, callback);
	}

	emit(...args) {
		this.ddpConnection.call(this.subscriptionName, ...args);
	}
};
