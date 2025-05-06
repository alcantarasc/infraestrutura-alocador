# Configuração SSL para OpenSearch

## Gerando os certificados

1. Primeiro, certifique-se de que você tem o OpenSSL instalado
2. Execute o script para gerar os certificados:
   ```bash
   # No Windows (Git Bash ou WSL)
   bash generate-certs.sh

   # No Linux/Mac
   chmod +x generate-certs.sh
   ./generate-certs.sh
   ```

3. Os certificados serão gerados na pasta `certs/`

## Estrutura dos certificados

- `root-ca.pem`: Certificado da autoridade certificadora raiz
- `root-ca-key.pem`: Chave privada da CA raiz
- `node.pem`: Certificado do nó OpenSearch
- `node-key.pem`: Chave privada do nó
- `admin.pem`: Certificado do admin
- `admin-key.pem`: Chave privada do admin

## Acessando o OpenSearch

- OpenSearch API: https://localhost:9200 (acesso interno)
- OpenSearch Dashboards: https://seu-ip-publico:5601 (acesso público)

### Credenciais
- Usuário: admin
- Senha: opensearch_secure_123

### Acesso Público ao Dashboard
O OpenSearch Dashboards está configurado para ser acessível publicamente através de HTTPS:
- URL: https://seu-ip-publico:5601
- Substitua "seu-ip-publico" pelo IP público do seu servidor
- A porta 5601 precisa estar liberada no firewall/security group do seu servidor

**Nota**: Como estamos usando certificados auto-assinados, seu navegador mostrará um aviso de segurança. Em ambiente de produção, recomenda-se:
1. Usar um certificado válido de uma autoridade certificadora (CA)
2. Ou usar Let's Encrypt (gratuito) se você tiver um domínio
3. Ou configurar um proxy reverso (como Nginx) com SSL gerenciado 