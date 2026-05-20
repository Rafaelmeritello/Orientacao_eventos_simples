console.log('Detector preços sendo executado')
setInterval(() => {}, 1000 * 60 * 60);
const amqp = require('amqplib');
const rabbitUrl = 'amqp://user:password@rabbitmq:5672';

let conexaoGlobal = null;
let canalGlobal = null;

let estaConectando = false; 

async function conectar() {
    
    if (estaConectando) return; 
    estaConectando = true;

    try {
        console.log("Conectando...");

        
        if (conexaoGlobal) {
            try { await conexaoGlobal.close(); } catch(e) {}
            conexaoGlobal = null;
        }

        conexaoGlobal = await amqp.connect(rabbitUrl);
        canalGlobal = await conexaoGlobal.createChannel();
        await canalGlobal.assertQueue('fila_precos', { durable: true });

        console.log("Conexão foi");
        estaConectando = false; 

    } catch (erro) {
        console.error("Falha ao conectar:", erro.message);
        canalGlobal = null;
        conexaoGlobal = null;
        estaConectando = false; 
        setTimeout(conectar, 5000);
    }
}

async function consumir_fila(){
    try{
    await canalGlobal.assertQueue('fila_precos',{ durable: true })
    canalGlobal.consume('fila_precos',(msg)=>{
        if(msg !=null){
            console.log('mensagem aqui',msg)
           canalGlobal.ack(msg)
        }
    })
    }catch(e){
        console.error('erro ao consumir', e)
    }

}

setTimeout(() => {
    conectar();
}, 15000);

setTimeout(() => {
   consumir_fila();
}, 21000);
