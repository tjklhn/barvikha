const axios = require('axios');
const geoip = require('geoip-lite');
const net = require('net');
const { HttpProxyAgent } = require('http-proxy-agent');
const { HttpsProxyAgent } = require('https-proxy-agent');
const { SocksProxyAgent } = require('socks-proxy-agent');
const { exec } = require('child_process');
const { promisify } = require('util');

const execAsync = promisify(exec);

class ProxyChecker {
  constructor() {
    this.timeout = 10000; // 10 секунд таймаут
    this.testUrls = [
      'https://ipinfo.io/json',
      'https://api64.ipify.org?format=json',
      'https://ip-api.com/json'
    ];
  }

  buildProxyUrl(proxyConfig) {
    if (!proxyConfig) return null;

    const rawType = (proxyConfig.type || 'http').toLowerCase();
    if (!proxyConfig.host || !proxyConfig.port) return null;

    const auth = proxyConfig.username && proxyConfig.password
      ? `${encodeURIComponent(proxyConfig.username)}:${encodeURIComponent(proxyConfig.password)}@`
      : '';

    const type = rawType === 'socks5' ? 'socks5h' : rawType;
    return `${type}://${auth}${proxyConfig.host}:${proxyConfig.port}`;
  }

  buildAxiosConfig(proxyConfig, timeoutOverride) {
    const timeout = timeoutOverride || this.timeout;
    const axiosConfig = {
      timeout,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      },
      validateStatus: (status) => status >= 200 && status < 400
    };

    const proxyUrl = this.buildProxyUrl(proxyConfig);
    if (proxyUrl) {
      const proxyType = (proxyConfig.type || '').toLowerCase();
      if (proxyType.startsWith('socks')) {
        const agent = new SocksProxyAgent(proxyUrl);
        axiosConfig.httpAgent = agent;
        axiosConfig.httpsAgent = agent;
      } else {
        axiosConfig.httpAgent = new HttpProxyAgent(proxyUrl);
        axiosConfig.httpsAgent = new HttpsProxyAgent(proxyUrl);
      }
      axiosConfig.proxy = false;
    }

    return axiosConfig;
  }

  // Проверка пинга (только для Windows)
  async checkPing(host) {
    try {
      const { stdout } = await execAsync(`ping -n 1 ${host}`);
      const match = stdout.match(/time[=<](\d+)ms/);
      return match ? parseInt(match[1]) : null;
    } catch (error) {
      return null;
    }
  }

  // Определение локации по IP
  getLocationByIP(ip) {
    try {
      const geo = geoip.lookup(ip);
      if (geo) {
        return {
          country: geo.country,
          city: geo.city,
          region: geo.region,
          timezone: geo.timezone,
          coordinates: [geo.ll[0], geo.ll[1]]
        };
      }
      return null;
    } catch (error) {
      return null;
    }
  }

  // Реальная проверка подключения к прокси (TCP + HTTP CONNECT)
  async checkProxyConnection(proxyConfig, targetHost = 'example.com', targetPort = 443) {
    if (!proxyConfig || !proxyConfig.host || !proxyConfig.port) {
      return {
        ok: false,
        error: 'Не указан хост или порт прокси'
      };
    }

    const proxyType = (proxyConfig.type || '').toLowerCase();
    const socketStart = Date.now();

    return new Promise((resolve) => {
      const socket = net.connect(
        { host: proxyConfig.host, port: proxyConfig.port },
        () => {
          const connectTime = Date.now() - socketStart;

          if (proxyType.startsWith('socks')) {
            socket.end();
            resolve({
              ok: true,
              tcpConnected: true,
              connectTime,
              note: 'SOCKS прокси: проверено только TCP-соединение (без handshake)'
            });
            return;
          }

          const authHeader = proxyConfig.username && proxyConfig.password
            ? `Proxy-Authorization: Basic ${Buffer.from(`${proxyConfig.username}:${proxyConfig.password}`).toString('base64')}\r\n`
            : '';

          const connectRequest = [
            `CONNECT ${targetHost}:${targetPort} HTTP/1.1`,
            `Host: ${targetHost}:${targetPort}`,
            'User-Agent: ProxyChecker/1.0',
            'Proxy-Connection: Keep-Alive',
            authHeader.trimEnd()
          ].filter(Boolean).join('\r\n') + '\r\n\r\n';

          socket.write(connectRequest);

          socket.once('data', (data) => {
            const responseText = data.toString('utf8');
            const statusLine = responseText.split('\n')[0] || '';
            const match = statusLine.match(/HTTP\/\d\.\d\s+(\d+)/);
            const statusCode = match ? parseInt(match[1], 10) : null;

            socket.end();

            if (statusCode && statusCode >= 200 && statusCode < 300) {
              resolve({
                ok: true,
                tcpConnected: true,
                connectTime,
                statusCode
              });
              return;
            }

            resolve({
              ok: false,
              tcpConnected: true,
              connectTime,
              statusCode,
              error: `Прокси вернул статус ${statusCode || 'неизвестный'}`
            });
          });
        }
      );

      socket.setTimeout(this.timeout, () => {
        socket.destroy();
        resolve({
          ok: false,
          error: 'Таймаут соединения с прокси'
        });
      });

      socket.on('error', (error) => {
        resolve({
          ok: false,
          error: `Ошибка соединения с прокси: ${error.message}`
        });
      });
    });
  }

  // Проверка прокси через внешние сервисы
  async checkProxy(proxyConfig) {
    const startTime = Date.now();

    const connectionCheck = await this.checkProxyConnection(proxyConfig);
    if (!connectionCheck.ok) {
      return {
        success: false,
        error: connectionCheck.error || 'Прокси не отвечает',
        timestamp: new Date().toISOString(),
        responseTime: Date.now() - startTime,
        connectionCheck
      };
    }

    const axiosConfig = this.buildAxiosConfig(proxyConfig);

    try {
      // Пробуем разные сервисы для проверки
      let response;
      let serviceUsed = '';

      const client = axios.create(axiosConfig);
      const directClient = axios.create({
        timeout: this.timeout,
        headers: axiosConfig.headers,
        validateStatus: axiosConfig.validateStatus,
        proxy: false
      });

      for (const url of this.testUrls) {
        try {
          response = await client.get(url);
          serviceUsed = url;
          break;
        } catch (error) {
          continue;
        }
      }

      if (!response) {
        throw new Error('Все сервисы недоступны через прокси');
      }

      const endTime = Date.now();
      const responseTime = endTime - startTime;

      // Извлекаем данные о IP
      let ip, country, city, isp;

      if (serviceUsed.includes('ip-api.com')) {
        ip = response.data.query;
        country = response.data.country;
        city = response.data.city;
        isp = response.data.isp;
      } else if (serviceUsed.includes('ipinfo.io')) {
        ip = response.data.ip;
        country = response.data.country;
        city = response.data.city;
        isp = response.data.org;
      } else if (serviceUsed.includes('ipify.org')) {
        ip = response.data.ip;
        // Для этого сервиса получаем геоданные отдельно
        const geoResponse = await client.get(`https://ip-api.com/json/${ip}`);
        country = geoResponse.data.country;
        city = geoResponse.data.city;
        isp = geoResponse.data.isp;
      }

      const directIps = await this.getDirectIps(directClient);

      if (directIps.length > 0 && ip && directIps.includes(ip)) {
        return {
          success: false,
          error: 'Прокси не используется: IP совпадает с реальным',
          timestamp: new Date().toISOString(),
          responseTime: Date.now() - startTime,
          connectionCheck
        };
      }

      // Дополнительная проверка геоданных
      const geoData = this.getLocationByIP(ip);

      // Проверяем пинг до прокси сервера
      const ping = await this.checkPing(proxyConfig.host);

      return {
        success: true,
        ip: ip,
        location: {
          country: country || geoData?.country || 'Неизвестно',
          city: city || geoData?.city || 'Неизвестно',
          region: geoData?.region,
          timezone: geoData?.timezone
        },
        isp: isp || 'Неизвестный провайдер',
        responseTime: responseTime,
        ping: ping,
        serviceUsed: serviceUsed,
        timestamp: new Date().toISOString(),
        proxyType: proxyConfig.type,
        rawData: response.data,
        connectionCheck
      };

    } catch (error) {
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
        responseTime: Date.now() - startTime,
        connectionCheck
      };
    }
  }

  async getDirectIps(directClient) {
    const ips = new Set();
    const checks = [
      async () => {
        const response = await directClient.get('https://api.ipify.org?format=json');
        return response?.data?.ip;
      },
      async () => {
        const response = await directClient.get('https://api64.ipify.org?format=json');
        return response?.data?.ip;
      },
      async () => {
        const response = await directClient.get('https://ipinfo.io/json');
        return response?.data?.ip;
      }
    ];

    for (const check of checks) {
      try {
        const ip = await check();
        if (ip) {
          ips.add(ip);
        }
      } catch (error) {
        continue;
      }
    }

    return Array.from(ips);
  }

  // Массовая проверка прокси
  async checkMultipleProxies(proxies) {
    const results = [];

    for (const proxy of proxies) {
      const result = await this.checkProxy(proxy);
      results.push({
        proxyId: proxy.id,
        ...result
      });
      // Задержка между проверками
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    return results;
  }

  // Быстрая проверка доступности
  async quickCheck(proxyConfig) {
    const connectionCheck = await this.checkProxyConnection(proxyConfig);
    if (!connectionCheck.ok) {
      return {
        available: false,
        error: connectionCheck.error,
        timestamp: new Date().toISOString(),
        connectionCheck
      };
    }

    try {
      const axiosConfig = this.buildAxiosConfig(proxyConfig, 5000);
      const client = axios.create(axiosConfig);
      await client.get('http://www.google.com', { timeout: 5000 });

      return {
        available: true,
        timestamp: new Date().toISOString(),
        connectionCheck
      };
    } catch (error) {
      return {
        available: false,
        error: error.message,
        timestamp: new Date().toISOString(),
        connectionCheck
      };
    }
  }
}

module.exports = new ProxyChecker();
