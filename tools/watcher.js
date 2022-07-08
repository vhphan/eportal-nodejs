const fs = require('fs');

function watch(dir) {
    console.log('start watching', dir);
    const watcher = fs.watch(dir, {
        persistent: true,
        recursive: true
    }, function (event, filename) {
        watcher.close();
        console.log('1', event, filename);
        setTimeout(() => {
            watch(dir);
        }, 10_000)
    });
}
// Celcom my edb /home/eproject/veehuen/myedb/Myedb/Myedb/*.rar
watch('C:\\Users\\eveepha\\PycharmProjects\\eportal-nodejs\\temp');  //<- watching the ./watchme/ directory