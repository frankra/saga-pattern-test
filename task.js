module.exports = async function task(str){
    str = str + '';
    return new Promise(resolve=>{
        const timeout = (Math.random() * 1000);
        
        setTimeout(()=>{
            console.log(`timeout: ${timeout}ms`);
            resolve(str.split().reverse().join(''));
        }, timeout);
    })
}