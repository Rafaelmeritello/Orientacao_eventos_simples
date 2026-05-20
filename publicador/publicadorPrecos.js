console.log('Publicador preços sendo executado');

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
function enviarMensagemPreco() {

    if (!canalGlobal) {
        console.log("Canal não está pronto ainda");
        return;
    }

    try {
        const dadosPreco = {
            produto: 'Teclado',
            preco: 299.90,
            timestamp: new Date().toISOString()
        };
        const mensagem = JSON.stringify(dadosPreco);

        canalGlobal.sendToQueue('fila_precos', Buffer.from(mensagem), { persistent: true });
        console.log(`Enviado : ${mensagem}`);
    } catch (erro) {
        console.error(" Erro ao enviar mensagem :", erro.message);
    }
}

setTimeout(() => {
    conectar();
}, 15000);

setInterval(enviarMensagemPreco, 8000);