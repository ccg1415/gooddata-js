export default class Rules {
    constructor() {
        this.rules = [];
    }

    addRule(tests, callback) {
        this.rules.push([tests, callback]);
    }

    match(subject) {
        const [,callback] = find(this.rules, ([tests]) => tests.every(test => test(subject)));

        invariant(callback, 'Callback not found :-(');

        return callback;
    }
}
