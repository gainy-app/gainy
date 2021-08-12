exports.getInput = (event) => {
    const body = JSON.parse(event.body);
    if (!body) {
        throw new Error('empty body');
    }
    const { input } = body;
    if (!input) {
        throw new Error('input is unset');
    }

    return input;
}