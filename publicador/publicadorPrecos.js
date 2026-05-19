console.log('Publicador preços sendo executado')

const amqp = require('amqplib');
const rabbitUrl = 'amqp://user:password@rabbitmq:5672';

let canalGlobal = null;
let conexaoGlobal = null;
async function iniciarRabbitMQ() {
    try {
        conexaoGlobal = await amqp.connect(rabbitUrl);
        canalGlobal = await conexaoGlobal.createChannel();
        await canalGlobal.assertQueue('fila_precos', { durable: true });
        console.log("Conexão global com RabbitMQ estabelecida com sucesso!");
    } catch (erro) {
        console.error("Falha ao iniciar conexão com RabbitMQ:", erro.message);
       
        setTimeout(iniciarRabbitMQ, 5000);
    }
}
async function publicar() {
 if (!canalGlobal) {
        console.log("Canal ainda não está pronto. Pulando");
        return;
    }

    try {
        const dadosPreco = {
            produto: 'Teclado',
            preco: 299.90,
            
        };
        const mensagem = JSON.stringify(dadosPreco);

     
        canalGlobal.sendToQueue('fila_precos', Buffer.from(mensagem), { persistent: true });
        console.log(`Enviado com sucesso: ${mensagem}`);
    } catch (erro) {
        console.error("Erro ao enviar:", erro.message);
    }
}
iniciarRabbitMQ().then(() => {
  
    setInterval(publicar, 5000);
});
setInterval(() => {}, 1000 * 60 * 60);