const util = require('util');
const debug = util.debuglog('batched-queue');
const EventEmitter = require('events');

const DEFAULT_FLUSH_LIMIT = 10000;
const DEFAULT_INTERVAL_THRESHOLD = 1000;
const DEFAULT_CARGO_LIMIT = 1;
const SAFETY_QUEUE_LIMIT = 2e6;

/**
 * Creates a BatchedQueue object which internally has a queue that will flush at `interval` ms or when queue length has reached `limit` (as defined in `options`)
 * You can optionally limit the flushing to only flush `payload` number of values per flush
 * @param { object } options The configuration object for this BatchedQueue
 */
class BatchedQueue extends EventEmitter {
    constructor(options) {
        options = options || {};
        this._q = [];
        this._flushLimit = options.limit || DEFAULT_FLUSH_LIMIT;
        this._intervalThreshold = options.interval || DEFAULT_INTERVAL_THRESHOLD;
        if (typeof options.cargo === 'function') {
            this._cargoMode = true;
            this._cargoLimit = options.cargoLimit || DEFAULT_CARGO_LIMIT;
            this._cargoFn = options.cargo;
        } else {
            this._cargoMode = false;
        }
        this._safetyLimit = options.safetyLimit || SAFETY_QUEUE_LIMIT;
        this._flushing = true;
    }

    get length() {
        return this._q.length;
    }

    get saturated() {
        if (this._cargoMode) {
            return this._q.length >= this._cargoLimit;
        } else {
            return this._q.length >= this._flushLimit;
        }
    }

    /**
     * Pushes an object onto the end of BatchedQueue queue for processing
     */
    push(val) {
        if (this._q.length > this._safetyLimit) {
            debug('BatchedQueue reached the safety limit of ' + SAFETY_QUEUE_LIMIT + ', further data added will be dropped!');
            return this;
        }
        this._q.push(val);
        this._flushCheck();
        return this;
    }

    /**
     * Push an object onto the beginning of the BatchedQueue queue for processing
     */
    unshift(val) {
        if (this._q.length > this._safetyLimit) {
            debug('BatchedQueue reached the safety limit of ' + SAFETY_QUEUE_LIMIT + ', further data added will be dropped!');
            return this;
        }
        this._q.unshift(val);
        this._flushCheck();
        return this;
    }

    /**
     * Quick way to completely empty the contents in the queue
     */
    empty() {
        this._q.length = 0;
        this._clearTimer();
        return this;
    }

    /**
     * Pauses the queue from flushing
     */
    pause() {
        this._flushing = false;
        this._clearTimer();
        return this;
    }

    /**
     * Resumes the queue to begin flushing again
     */
    resume() {
        this._flushing = true;
        this._flushCheck();
        return this;
    }

    /**
     * Internal functions
     */
    _flush() {
        this._clearTimer();
        var saturated = this.saturated;
        if (saturated) {
            debug('BatchedQueue flushing due to saturated queue (%d)', this._q.length);
        } else {
            debug('BatchedQueue flushing due to interval reached (%d)', this._intervalThreshold);
        }
        if (this.cargoMode && !isNaN(this._cargoLimit) && this._cargoLimit > 0) {
            this._cargoFn.call(this, this._q.splice(0, this._cargoLimit), this.resume.bind(this));
            this.pause();
        } else {
            this.emit('flush', this._q, saturated);
            this._q.length = 0;
        }
    }

    _flushCheck() {
        if (!this._flushing) {
            return;
        }
        if (this.saturated) {
            this._flush();
            return;
        }
        if (!this._timer) {
            this._timer = setTimeout(this._flush.bind(this), this._intervalThreshold);
        }
    }

    _clearTimer() {
        if (this._timer) {
            clearTimeout(this._timer);
            delete this._timer;
        }
    }
}

module.exports = BatchedQueue;
