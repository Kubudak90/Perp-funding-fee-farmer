# Perp Funding Fee Farmer (Arcus ⇄ Lighter)

Arcus ile Lighter perp DEX'leri arasında **delta-nötr** çalışan bir funding ve hacim botu.

- **Arcus** maker bacağıdır. Emirler post-only (ALO) gönderilir ve maker ücreti **0 ppm**'dir.
- **Lighter** hedge bacağıdır. Her Arcus dolumu hemen Lighter'da ters yönlü bir **taker IOC** emirle kapatılır. Lighter
  **Standard** hesapta işlem ücreti **0**'dır.

Botun iki amacı vardır:

1. **Funding taşıma getirisi (carry).** İki borsanın saatlik funding oranı farklı olduğunda bot, farkın lehine olduğu
   yönde pozisyon tutar. Örneğin Arcus kısa / Lighter uzun pozisyonu saatte `r_Arcus - r_Lighter` kazandırır.
2. **İşlem hacmi.** Bot pozisyonu sürekli açıp kapatır (aç → tut → kapat → yeniden aç). Böylece iki borsada da hacim
   oluşur, bu da puan veya ödül programları için işe yarayabilir.

> **Bu yazılım kâr garantisi vermez.** Funding oranları her saat değişir. Fiyat kayması, borsa kuralları, likidasyon
> ve yazılım hataları para kaybettirebilir. Önce `--paper` modunda deneyin, sonra küçük tutarlarla başlayın.

---

## İçindekiler

1. [Nasıl çalışır?](#nasıl-çalışır)
2. [Maliyet modeli ve gerçekçi beklentiler](#maliyet-modeli-ve-gerçekçi-beklentiler)
3. [ÖNEMLİ UYARILAR](#önemli-uyarilar)
4. [Kurulum](#kurulum)
5. [İlk çalıştırma (önerilen sıra)](#i̇lk-çalıştırma-önerilen-sıra)
6. [Komutlar](#komutlar)
7. [Web paneli (dashboard)](#web-paneli-dashboard)
8. [Varsayılan ayarlar ($200 / borsa)](#varsayılan-ayarlar-200--borsa)
9. [Yapılandırma: öne çıkanlar](#yapılandırma-öne-çıkanlar)
10. [İzleme ve Telegram](#i̇zleme-ve-telegram)
11. [Sorun giderme ve hata kodları](#sorun-giderme-ve-hata-kodları)
12. [Geliştirici notları](#geliştirici-notları)

---

## Nasıl çalışır?

Bot her piyasayı (ör. BTC) bağımsız bir çift olarak yönetir: **Arcus BTC-USD ⇄ Lighter BTC**.

1. **Yön seçimi (FundingMonitor).** Bot iki borsanın funding tahminlerini dakikada bir okur ve kazandıran yönü seçer.
   - `+1`: Arcus UZUN / Lighter KISA.
   - `-1`: Arcus KISA / Lighter UZUN.

   Yön değişikliği histerezisle yapılır (`flip_hysteresis_apr`). Yön ancak pozisyon düzken değişir.
2. **OPENING.** Arcus'a post-only maker emir verilir. Fiyat şöyle hesaplanır: emir dolar dolmaz Lighter'da o anki
   defter VWAP'ıyla hedge edildiğinde en az `min_edge_bps` kâr kalmalıdır. Kriptoda iki borsa arasında yapısal bir fiyat
   farkı (baz) vardır. Bot bu farkı bir EMA ile takip eder ve hesaba katar.
3. **Hedge (Hedger).** Her Arcus dolumu ~250 ms içinde Lighter'da ters yönlü IOC emirle hedge edilir. Bot şu kuralı
   sürekli korur: `arcus_pozisyon + lighter_pozisyon + yoldaki_hedge = 0`. Pozisyonlar düzenli olarak borsadan okunup
   doğrulanır (reconcile).
4. **HOLDING.** Hedef büyüklüğe (`target_notional`) ulaşınca kotasyon durur.
   - Pozisyon `hold_seconds` boyunca tutulur.
   - `hold_through_funding: true` ise bir sonraki saat başı funding ödemesi de beklenir.
5. **CLOSING.** Pozisyon yine post-only (reduce-only) maker emirlerle `churn_floor_frac` seviyesine kadar küçültülür.
   Her dolum yine hedge edilir. Ardından döngü OPENING ile yeniden başlar.
   - Funding ödemesine `protect_funding_window_s` saniye kala kapanış ertelenir. Böylece ödeme anında pozisyon açık
     kalır.

**Döngü başına hacim:** her borsada `2 × (target - floor)` notional. Örnek: varsayılan `target_notional: 150` ve
floor 0 ile her tam döngü, piyasa başına Arcus'ta yaklaşık $300, Lighter'da da yaklaşık $300 hacim üretir.

**Güvenlik mekanizmaları:**

- Hedge her zaman serbesttir; risk duraklatması bile hedge'i engellemez.
- Şu durumlardan biri olursa kotasyon durur:
  - defter eskidi (`stale_book_s`),
  - fiyatlar iki borsa arasında ayrıştı (`max_divergence_bps`, `max_basis_dev_bps`),
  - hedgelenmemiş delta var (`max_unhedged_notional`),
  - marj oranı yüksek (`max_margin_ratio`) ya da serbest teminat yetersiz (`min_free_collateral_mult`),
  - RWA piyasası işlem saatleri dışında,
  - günlük zarar limiti aşıldı (`max_daily_loss`),
  - art arda çok hata oldu (devre kesici). Devre kesici halt'ı `error_halt_cooldown_s` (300 sn) boyunca hiç hata
    gelmezse kendiliğinden kalkar. Bekleme süresi her otomatik devamda iki katına çıkar. 24 saatte
    `max_auto_resumes_per_day` (3) kez devam ettikten sonra bot yeniden başlatılana kadar durur. Günlük zarar,
    marj ve hedge kaynaklı halt'lar kendiliğinden kalkmaz.
- Açılışta hiçbir çiftin yönetmediği pozisyonlar (config dışı piyasalar, eşlenemeyen ya da reduce-only piyasalar)
  log ve Telegram ile bildirilir. Bot bu pozisyonları ne hedge eder ne kapatır.
- Arcus **dead man's switch** kullanılır. Bot ölürse borsa, `dead_mans_switch_s` saniye sonra tüm emirleri iptal eder.
- Kapanışta (Ctrl+C / SIGTERM) kotasyonlar iptal edilir ve açık delta hedge edilir. `close_on_exit: true` ise iki
  bacak da kapatılır.

## Maliyet modeli ve gerçekçi beklentiler

Rakamlar 2026-10-19 tarihli canlı ölçümlerden ve 7 günlük geriye dönük testten gelir. Piyasa koşulları değişir.

**Döngü (round trip) başına maliyet, tek bacak notional'ının bps'i cinsinden:**

```
maliyet ≈ Lighter_etki(alış) + Lighter_etki(satış) - Arcus_spread + ters seçim (adverse selection)
```

- Büyük piyasalarda (BTC, ETH, SOL, SPY, QQQ, NVDA) gerçekçi maliyet **≈ 0,2 – 1,5 bps / döngü** olur, ters seçim
  dahil.
- Bu, Arcus'taki her **$1M hacim için yaklaşık $10 – $75** demektir. Bir döngü tek bacak notional'ının 2 katı kadar
  Arcus hacmi üretir; yani $1M Arcus hacmi ≈ $500k'lık döngü × 0,2–1,5 bps. Lighter'da da aynı hacim ücretsiz oluşur.
- `farmer stats` bu değeri iki şekilde basar: Arcus (maker borsası) hacmi başına ve iki borsanın toplam hacmi başına.
  İkincisi yaklaşık yarısıdır. Yukarıdaki bantla **Arcus hacmi başına** olanı karşılaştırın.
- Lighter Standard taker gecikmesi ~300 ms'dir. Arcus dolumu ile hedge arasında fiyat kayabilir (ters seçim). Bu
  maliyet gerçek işlemde tahminin 1–3 katı olabilir.
- Lighter'da en iyi fiyat seviyesindeki derinlik küçüktür (BTC'de bazen < $100). Büyük clip'ler defterin içine yürür ve
  maliyet artar. `farmer scan` çıktısındaki `RT@$...` sütunu bir clip için bu tahmini gösterir.

**Funding taşıma getirisi (APR, hedgelenmiş notional üzerinden, GARANTİ DEĞİL):**

- İki borsa da taban orandayken Arcus KISA / Lighter UZUN pozisyonu kriptoda **≈ %0,44 APR**, RWA'da işlem saatleri
  dışında **≈ %0,9 APR** kazandırır.
- 7 günlük geriye dönük test, karamsar ve iyimser senaryo aralığı:

  | Piyasa | APR aralığı |
  |---|---|
  | BTC | %0,7 – 1,5 |
  | ETH | %-2,2 – 3,5 |
  | SOL | %1,7 – 6,4 |
  | SPY | %3,7 – 6,5 |
  | QQQ | %4,1 – 7,8 |
  | NVDA | %-0,1 – 7 |
  | META | %8 – 15 |
  | AMD | %8 – 26 |

  İyimser uç, ileriye bakma (look-ahead) içerdiği için üst tahmindir.
- Örnek: varsayılan bacak başına $150 pozisyon, %2 APR ile yılda yaklaşık **$3** eder (iki piyasada ~$6). Carry tek
  başına küçüktür. Asıl "ürün" çoğunlukla **hacimdir**.

**Yapısal baz (kripto).** Arcus fiyatları USD, Lighter'ın kripto endeksi ise fiilen USDT bazlıdır. Bu yüzden Arcus orta
fiyatı Lighter'ın **~9–14 bps altındadır**.

- Çalışan botun PnL tahmini (periyodik rapor ve günlük zarar limiti) her bacağı kendi borsasının orta fiyatıyla
  değerler. Bu yüzden hedgelenmiş girişteki baz farkı zarar (ya da kâr) gibi görünmez.
- `farmer stats` ise açık net pozisyonu tek bir ortak (Arcus) fiyatla değerler. Orada fark, pozisyon kapanana kadar
  geçici bir "zarar" gibi görünebilir.
- Asıl risk USDT/USD kurunun oynamasıdır. 30 günlük aralık yaklaşık 12 bps'tir.

## ÖNEMLİ UYARILAR

1. **Lighter coğrafi kısıtlama (kod 20558).** Lighter, kısıtlı ülkelerden işlem göndermeyi (sendTx, kimlik
   doğrulamalı websocket) reddeder.
   - Botu **izin verilen bir yargı bölgesinden** çalıştırın.
   - Lighter kullanım şartlarını okuyun. Kısıtlamayı kurallara aykırı yollarla aşmaya çalışmayın.
   - Halka açık (readonly) veri her yerden okunabilir. `farmer scan` ve `--paper` bu yüzden kısıtlı bölgede de çalışır.
2. **Arcus mainnet erişimi beyaz listeye bağlıdır.** Adresiniz erişim listesinde değilse API `403 address not on access
   whitelist` döner. Erişim davet kodu veya erişim programıyla gelir. Testnet serbesttir.
3. **Lighter Standard hesap limitleri.**
   - Hesap başına **dakikada 60 istek** vardır. REST okumaları ve sendTx aynı kovayı paylaşır, işlem türü başına limit
     de ~40/dk'dır.
   - Ayrıca belgelenmemiş bir **işlem limiti (kod 52002)** vardır. Bu limite takılınca yalnızca pozisyon azaltan
     işlemlere izin verilir. Bot bu durumda yeni pozisyon açmayı `standard_limit_cooldown_s` boyunca durdurur.
   - Aynı API anahtarıyla **tek bir bot süreci** çalıştırın, çünkü nonce akışı paylaşılmaz.
4. **Puan/ödül programları yapay hacmi dışlayabilir.** Lighter'ın (ve muhtemelen Arcus'un) şartları wash trading veya
   "yapay" hacmi puanlardan çıkarabilir, hesabı diskalifiye edebilir. Bot kendi kendine işlem yapmaz, karşı taraf her
   zaman başka katılımcılardır. Yine de hacim odaklı stratejilerin değerlendirilmesi borsanın takdirindedir. Şartları
   okuyun.
5. **Likidasyon riski her bacakta ayrıdır.** Toplam delta sıfırdır, ama teminat iki borsada ayrı tutulur. Sert bir
   fiyat hareketinde bir bacak kâr ederken diğeri zarar eder ve o borsadaki teminat yetmezse **likide olabilir**.
   - Her iki borsada da yeterli teminat tutun.
   - **Düşük kaldıraç** kullanın (2–5x; varsayılan 3x).
   - Marj oranını (bakım marjı / özsermaye; 1,0 = likidasyon) izleyin: `farmer status`, Telegram raporları.
   - Risk yöneticisi `max_margin_ratio` (0,25) üstünde yeni pozisyon açmaz. Serbest teminat bir clip'in başlangıç
     marjını (`clip / kaldıraç × min_free_collateral_mult`) karşılamıyorsa da açmaz. Bu kontrol iki borsada da
     yapılır, böylece hedge bacağı marj yetersizliğinden reddedilmez.
   - `reduce_margin_ratio` (0,4; özsermaye < 2,5 × bakım marjı) üstünde bot durur (halt) ve tüm çiftlerin iki
     bacağını reduce-only taker (IOC) emirleriyle kapatır. Oran yüksek kaldıkça kapatma yeniden denenir. Tekrar işlem
     için botu yeniden başlatın.
   - Teminatı düzenli olarak dengeleyin. Bot borsalar arasında para transferi yapmaz. İki borsadaki özsermaye
     `max_equity_divergence_frac` (%30) oranından fazla ayrışırsa uyarı (log + Telegram) gelir.
6. **USDT/USD bazı (kripto).** Lighter'ın kripto endeksi USDT bazlıdır. USDT'nin USD'den sapması iki bacak arasında PnL
   farkı yaratır.
7. **RWA (hisse/ETF) piyasaları.**
   - Arcus'ta yalnızca normal işlem saatlerinde (`isOutsideRth == false`) kotasyon verilir.
   - Saat dışında marj gereksinimi artar (×1,5), fiyat bantları uygulanır.
   - Bazı piyasaların açık pozisyon tavanı (OI cap) doludur. `OPEN_INTEREST_CAP_EXCEEDED` hatası bu demektir.
8. **Yazılım riski.**
   - Gerçek emirle uçtan uca test yalnızca küçük tutarlarla yapılmalıdır.
   - Borsa API'leri değişebilir.
   - Lighter tarafında bazı davranışlar (52002 metni, modify semantiği) belgelenmemiştir.
   - Önce `--paper`, sonra çok küçük `target_notional` ile başlayın.

## Kurulum

Gereksinimler: Python **3.11+**, Linux veya macOS. `lighter-sdk` paketi yerel (native) bir imzalayıcı içerir.
Desteklenen platformlar için lighter-sdk belgelerine bakın.

```bash
git clone <repo> && cd Perp-funding-fee-farmer
python3 -m venv .venv
source .venv/bin/activate
pip install -e .            # geliştirme/test için: pip install -e '.[dev]'

cp config.example.yaml config.yaml
cp .env.example .env && chmod 600 .env
```

### API anahtarları

**Seçenek A: `farmer keygen`.** Cüzdan anahtarı yalnızca ortam değişkeninden okunur, hiçbir zaman ekrana yazılmaz ve
ağa gönderilmez. Sadece yerelde imza için kullanılır.

```bash
# Arcus: ana cüzdanla EIP-712 imzalı createApiKey (adres beyaz listede olmalı)
export ARCUS_WALLET_PRIVATE_KEY=0x...      # yalnızca bu komut için; sonra: unset ARCUS_WALLET_PRIVATE_KEY
farmer keygen arcus --account-index 0

# Lighter: L1 cüzdanla ChangePubKey (hesap önce para yatırılarak oluşturulmuş olmalı; izinli bölgeden çalıştırın)
export LIGHTER_ETH_PRIVATE_KEY=0x...
farmer keygen lighter --api-key-index 4
```

Komut, `.env` dosyasına yapıştırılacak satırları basar (`ARCUS_ADDRESS=...`, `ARCUS_API_PRIVATE_KEY=...`,
`LIGHTER_ACCOUNT_INDEX=...` gibi).

- **Üretilen API özel anahtarı yalnızca bir kez gösterilir.** Hemen `.env` dosyasına kaydedin.
- Kayıt gönderildi ama onayı beklenirken zaman aşımı olduysa anahtar yine de `NOT confirmed` uyarısıyla basılır
  (kayıt sonradan geçerli olabilir). `farmer status` ile doğrulayın; kayıt olmadıysa anahtarı atın.
- Çalışan botun kullandığı Lighter anahtar yuvası (`LIGHTER_API_KEY_INDEX`) `--force` olmadan değiştirilmez.
- `--generate-only` yalnızca yerelde anahtar çifti üretir, ağa bağlanmaz.
- Arcus'ta aynı isimle yeniden anahtar oluşturmak eski anahtarı iptal eder. Varsayılan isim her anahtar için
  benzersizdir (`farmer-<pub8>`).
- Lighter'da 0–3 numaralı anahtar yuvaları Lighter uygulamalarına ayrılmıştır. 4–254 arasını kullanın.

**Seçenek B: elle.** Anahtarları borsaların web arayüzünden oluşturun ve `.env` dosyasına yazın. Alanların açıklaması
`.env.example` içindedir.

## İlk çalıştırma (önerilen sıra)

1. **Piyasa taraması.** Kimlik bilgisi gerekmez, yalnızca halka açık veri kullanır.
   ```bash
   farmer scan --top 20
   ```
   Çıktıda her çift için şunlar görünür:
   - funding (%/saat),
   - en iyi yönün carry APR'ı,
   - iki borsanın spread'i,
   - Lighter en iyi seviye derinliği ($),
   - baz,
   - bir clip için round-trip maliyet tahmini,
   - RWA işlem saati durumu.

   `*` işareti config'teki piyasaları gösterir.
2. **Paper (simülasyon).** Canlı veri kullanır, emirler simüledir:
   ```bash
   farmer run --paper --duration 600     # 10 dakika sonra kendiliğinden durur; Ctrl+C de güvenli durdurur
   farmer stats --paper --since 1
   ```
   Loglarda şunları görmelisiniz:
   - iki borsadan defter akışı,
   - ~30 sn baz ısınması (`basis warm-up`),
   - Arcus'a kotasyon,
   - `paper fill arcus ...` → `hedge lighter ...` → `paper fill lighter ...`,
   - deltanın 0'a dönmesi,
   - OPENING → HOLDING → CLOSING geçişleri.

   Sakin piyasada dolum az olabilir.

   Paper dolum modeli muhafazakârdır:
   - Görünen bir defter seviyesi bir kez tüketilir. Aynı seviye yeniden yayınlansa da (heartbeat, başka seviyede
     değişiklik) tekrar dolum olmaz; seviye büyürse yalnızca artan kısım yeni likiditedir. Bir seviye kaybolup
     1 saniye içinde aynı büyüklükle geri gelirse (ya da aynı büyüklük yan fiyata taşınırsa) aynı emir sayılır.
   - Dolumlar defter bazlıdır: kotasyonumuz gerçek defterde görünmediği için spread içinde (negatif
     `min_edge_bps`) verilen kotasyonlar canlıya göre daha kolay dolar.
   - Hedge borsasının istek bütçesi azalınca (`strategy.min_taker_budget`) kotasyonlar geçici olarak çekilir.
   - Lighter Standard bütçesi (ortak 60 istek/dk, 40 tx/dk, `tx_budget_wait_s`) taklit edilir
     (`paper.emulate_rate_limits`). Bütçe dolunca hedge'ler bekler ya da reddedilir, canlıdaki gibi.
   - Hesap canlıyı taklit eder: her borsada `paper.initial_balance` (varsayılan $200) bakiye; bakım marjı borsanın
     gerçek oranıyla hesaplanır (ör. ETH Arcus %2,7, Lighter %1,2).
3. **Küçük canlı deneme.** Varsayılanlar borsa başına ~$200 teminat içindir
   ([Varsayılan ayarlar](#varsayılan-ayarlar-200--borsa)). İki borsaya da en az bu kadar teminat yatırın. Daha
   temkinli başlamak için ilk saatlerde tek piyasa kullanabilirsiniz (`strategy.markets` altında yalnızca
   `- symbol: ETH`).
   ```bash
   farmer status                  # bakiyeler, pozisyonlar, açık emirler, çift başına delta
   farmer run --live --yes        # ya da config'te mode: live
   ```
4. Birkaç gün izleyin (`farmer stats`, Telegram, panel), sonra teminatı ve boyutları **birlikte, kademeli** artırın
   ([ölçekleme kuralı](#varsayılan-ayarlar-200--borsa)).

Botu durdurmak için **Ctrl+C** veya `kill -TERM <pid>` kullanın (SIGHUP da aynı şekilde işlenir; `nohup` ile yok
sayılmışsa dokunulmaz). Bot kotasyonları iptal eder, açık deltayı hedge eder ve kapanır. `farmer close-all` sinyalle
yarıda kesilirse de aynı güvenli kapanışı yapar ve hata koduyla çıkar.

- İkinci sinyal yalnızca uyarı verir.
- Üçüncü sinyal hemen çıkar. Arcus'taki emirler bu durumda dead man's switch ile iptal olur.

## Komutlar

| Komut | Açıklama |
|---|---|
| `farmer scan [--top N] [--skipped]` | Halka açık tarama. Tüm örtüşen piyasalar listelenir, config'tekiler önce örneklenir. Lighter defterleri readonly websocket'ten alınır, 60 req/dk bütçesi harcanmaz. |
| `farmer run [--paper\|--live] [--yes] [--duration SN]` | Motoru çalıştırır. `--live` için iki borsada anahtar ve `--yes` ya da `mode: live` gerekir. Paper verisi `data/farmer-paper.db`, canlı veri `data/farmer.db` dosyasına yazılır. |
| `farmer status` | Bakiye, marj oranı, pozisyonlar, açık emirler ve çift başına delta. `UNHEDGED` işareti hedgelenmemiş delta demektir. Yalnızca reduce-only emir kabul eden bir piyasadaki pozisyon ayrıca işaretlenir (`farmer close-all` ile kapatın). Canlı anahtar gerekir. |
| `farmer close-all [--taker] [--yes] [--timeout SN]` | Tüm çiftleri düzler. Varsayılan maker modudur (hedge'li post-only kapanış, ucuz ama yavaş). `--taker` iki bacakta da IOC ile hemen kapatır. Yalnızca reduce-only emir kabul eden piyasalar (ör. Lighter `force_reduce_only`) iki modda da iki bacakta reduce-only IOC ile kapatılır. Günlük zarar limiti close-all'ı durdurmaz; risk yöneticisi başka bir sebeple durursa maker kapanış kendiliğinden taker moduna geçer. Hiç emir kabul etmeyen (ör. Arcus `OFFLINE`) ya da eşlenemeyen (fiyat kontrolü başarısız) bir çiftteki pozisyon `UNMANAGED` olarak listelenir ve komut hata koduyla çıkar: bu pozisyonu borsada elle kapatın. Çalışan bir bot varken çalışmaz (kilit dosyası). |
| `farmer dashboard [--host 127.0.0.1] [--port 8080]` | Yerel web paneli: durum, ayarlar, API anahtarları, başlat/durdur/close-all. Aşağıya bakın. |
| `farmer stats [--since SAAT] [--paper\|--live] [--no-marks]` | Veritabanından rapor: borsa başına hacim (maker/taker), ücret, funding, rol bazında dolumlar, PnL tahmini ve $1M hacim başına PnL (Arcus hacmi başına ve iki borsanın toplam hacmi başına). `--since 0` tüm zamanlar demektir. |
| `farmer keygen arcus\|lighter` | API anahtarı üretir ve kaydeder (yukarıya bakın). |

Ortak seçenekler:

- `--config config.yaml`: dosya yoksa varsayılanlar kullanılır.
- `--env-file .env`
- `--log-level DEBUG`

Loglar konsola ve `data/logs/farmer.log` dosyasına yazılır. Dosya döner: 50 MB × 5.

`python -m farmer ...` de aynı şekilde çalışır.

## Web paneli (dashboard)

```bash
farmer dashboard            # http://127.0.0.1:8080/#token=... adresini konsola basar
```

Konsola basılan bağlantıyı tarayıcıda açın. Bağlantıdaki `token` panele erişim anahtarıdır ve her açılışta yeniden
üretilir. Sabit bir anahtar için `.env` içine `FARMER_DASHBOARD_TOKEN=...` yazın. Bağlantıyı kimseyle paylaşmayın.

- **Genel bakış:** botun durumu ve halt sebebi; 1 sa / 24 sa / toplam hacim, PnL tahmini, funding ve ücretler;
  borsa başına bakiye ve marj oranı; çift başına faz, yön, pozisyonlar, delta, kotasyon, carry APR ve duraklama
  sebebi; saatlik hacim ve oturumun PnL grafiği; son olaylar.
- **Kontrol:** `Paper başlat`; `Canlı başlat` (iki borsada anahtar ve onay kutusuna `CANLI` yazmak gerekir);
  `Durdur` (Ctrl+C ile aynı güvenli kapanış: kotasyonlar iptal edilir, açık delta hedge edilir); `Tüm pozisyonları
  kapat` (maker ya da taker, onay için `KAPAT`); `Bağlantıyı test et` (`farmer status` çıktısı).
- **Ayarlar:** strateji, risk ve piyasa ayarları. Kaydetmeden önce bütün yapılandırma doğrulanır. `config.yaml`
  yeniden yazılır, önceki dosya `config.yaml.bak` olarak saklanır (yorum satırları korunmaz). Değişiklikler bir
  sonraki başlatmada geçerli olur.
- **API anahtarları:** Arcus ve Lighter API anahtarları, Telegram bilgileri. Değerler kaydetmeden önce kontrol
  edilir ve `.env` dosyasına (izin 600) yazılır. Kayıtlı gizli anahtarlar panelde bir daha gösterilmez.
  **Ana cüzdan özel anahtarları panele girilmez:** API anahtarı üretmek için `farmer keygen` kullanın.
- **Log:** son 2000 log satırı, seviye ve metin filtresiyle.

Bot panelin içinde çalışır: paneli durdurmak (Ctrl+C / SIGTERM) önce botu güvenli şekilde durdurur. Panelden
başlatılan bot, `farmer run` ile aynı kilit dosyasını kullanır; aynı veritabanında ikinci bir bot başlatılamaz.

**Güvenlik:** Panel varsayılan olarak yalnızca `127.0.0.1` adresini dinler ve bağlantı şifresizdir (HTTP). Botu bir
sunucuda çalıştırıyorsanız portu dışarı açmayın, SSH tüneli kullanın:

```bash
ssh -L 8080:127.0.0.1:8080 kullanici@sunucu   # sonra yerelde http://127.0.0.1:8080/#token=...
```

Her API isteği token ister; başka sitelerden gelen istekler (Origin / Host kontrolü) reddedilir. Sayfa dışarıdan hiçbir
script ya da dosya yüklemez.

## Varsayılan ayarlar ($200 / borsa)

Varsayılanlar, **her borsada yaklaşık $200 teminatı** (Arcus USDG, Lighter USDC) olan küçük bir hesap için
ayarlıdır. Öncelik sırası: önce güvenlik (likidasyon riski yok, bot kilitlenmez), sonra ~sıfır maliyetle en çok hacim;
funding ek gelirdir. Rakamlar 2026-10-21 tarihli halka açık borsa verilerinden ve kısa paper koşularından gelir.

| Alan | Değer | Neden |
|---|---|---|
| `strategy.markets` | ETH, SOL | $50 clip ile en çok dolum ve en düşük round-trip maliyet (SOL ~0,2 bps, ETH ~0,6 bps). BTC'nin Arcus defteri tek tick genişliğinde ve önünde ~$450k kuyruk var; hareketli piyasada SOL'un ~1/4'ü kadar dolum aldı. |
| `strategy.target_notional` | 150 | Piyasa başına 3 clip. Döngü tepesinde borsa başına en fazla ~$300 pozisyon (~1,5x efektif kaldıraç). |
| `strategy.order_notional` | 50 | Lighter'ın $10 asgari emrinin ~5 katı; Lighter defter etkisi ~0,3–0,4 bps. |
| `strategy.leverage` | 3 | Çapraz marjda likidasyon fiyatını değiştirmez. Ayrılan başlangıç marjını ($300 / 3 = $100) ve borsanın izin verdiği tavanı (özsermaye × 3 = $600) belirler. |
| `risk.max_total_notional` | 350 | 2 × 150 + bir clip: fiyat döngü içinde ~%15 oynasa da son clip açılır. |
| `risk.max_unhedged_notional` | 40 | Bir clip'ten küçük: 1 sn içinde hedgelenmeyen tam bir dolum kotasyonu durdurur ve hedge'i 2x kayma ile gönderir. Lighter asgarisinin (~$10) altındaki kırıntı ve küçük kısmi dolumlar alarm vermez. |
| `risk.max_daily_loss` | 15 | İki borsadaki toplam teminatın (~$400) ~%4'ü. |
| `paper.initial_balance` / `paper.default_leverage` | 200 / 3 | Paper, canlı hesabı taklit eder. |

Oransal ayarlar değişmedi ve her boyutta çalışır: `max_margin_ratio` 0,25, `reduce_margin_ratio` 0,4,
`min_free_collateral_mult` 1,5, `max_equity_divergence_frac` 0,3, `min_edge_bps` 0,5, `max_slippage_bps` 8.
`config.example.yaml` aynı değerleri açıklamalarıyla içerir. HYPE, SOL yerine seçilebilir (daha çok dolum ve daha
istikrarlı funding), ama Arcus bakım marjı %6,7 ve likidasyon ücreti %1'dir.

**Marj ve stres** (borsa başına $200, döngü tepesinde $300 pozisyon, 3x):

- Başlangıç marjı $100, serbest teminat $100. Yeni bir clip için gereken serbest teminat 50 / 3 × 1,5 = **$25**. Bu
  kontrol döngünün her adımında iki borsada da geçer.
- Marj oranı (bakım marjı / özsermaye) tepede Arcus'ta ~0,045, Lighter'da ~0,027'dir. Yeni pozisyon sınırı 0,25'tir.
- Fiyat tek yönde oynadığında zarar eden borsada ($300 tek yönlü pozisyon) olanlar:

  | Fiyat hareketi | Ne olur |
  |---|---|
  | ~%12 | İki borsanın özsermayesi %30'dan fazla ayrışır: uyarı gelir, teminatı dengeleyin. |
  | ~%19 (kısa bacak) / ~%37 (uzun bacak) | Serbest teminat yetmez: yeni clip açılmaz. |
  | ~%55 (Arcus kısa) / ~%65 (Lighter uzun) | `reduce_margin_ratio`: bot durur ve iki bacağı reduce-only taker emirle kapatır. |
  | ~%62 / ~%66 | Likidasyon. |

  Asıl koruma düşük efektif kaldıraçtır (~1,5x). Bot borsalar arasında para transferi yapmaz; teminatı düzenli
  dengeleyin.
- Tek borsada ~$90'a kadar birikmiş zararla da tam döngü açılabilir. Daha fazlasında yeni clip açılmaz ve hacim durur;
  teminatı dengeleyince bot kendiliğinden devam eder.

**Beklenen hacim ve maliyet** (kısa paper ölçümleri, garanti değil):

- ETH + SOL ile Arcus'ta hareketli piyasada saatte **~$10.000**, sakin piyasada saatte **~$200–600** maker hacmi.
  Lighter'da aynı kadar hedge hacmi oluşur.
- Hedge'ler piyasa başına dakikada 0,1–2 işlemdir; Lighter'ın 40 tx/dk sınırının ≤ %10'u.
- Kısa paper koşularında maliyet Arcus hacminin her $1M'ı için ~$70–165 oldu (−0,7 ile −1,7 bps). Bu oranla
  `max_daily_loss` (15) günde ~$90–200k Arcus hacminde dolar. Normal çalışmada tetikleniyorsa limiti değil
  `min_edge_bps` değerini yükseltin.
- Funding $150 pozisyonda günde $0,02'nin altındadır.
- Arcus emir havuzu dolan her $1 için 10 işlem hakkı kazandırır (bir $50 dolum = 500 hak). Hiç dolum olmayan sakin
  saatlerde iki piyasa saatte ~3.900 hak harcar; havuz yarıya inince bot fiyat iyileştirmelerini kendiliğinden
  yavaşlatır. Hareketli bir saat havuzu fazlasıyla doldurur.

**Ölçekleme.** Teminat artınca USD alanlarını teminatla aynı oranda büyütün. Kaldıraç ve oransal ayarlar değişmez.

| Alan | Kural | $200 / borsa | $1000 / borsa |
|---|---|---|---|
| `strategy.target_notional` | teminat × 1,5 / piyasa sayısı | 150 | 750 |
| `strategy.order_notional` | hedefin ~1/3'ü (Lighter derinliği: `farmer scan` → `RT@$`) | 50 | 250 |
| `risk.max_total_notional` | piyasa sayısı × hedef + bir clip | 350 | 1750 |
| `risk.max_unhedged_notional` | bir clip'in ~%80'i | 40 | 200 |
| `risk.max_daily_loss` | toplam teminatın ~%4'ü | 15 | 75 |
| `paper.initial_balance` | borsa başına teminat | 200 | 1000 |

Piyasa eklerken `risk.max_total_notional` değerini de artırın (ör. BTC eklenirse 3 × 150 + 50 = 500). Hedeflerin
toplamı artı bir clip bu sınırı aşarsa son piyasa hedefine ulaşamaz ve hacim üretmez; program başlarken logda,
panelde de ayarları kaydederken uyarı verir.

## Yapılandırma: öne çıkanlar

Tüm alanlar Türkçe açıklamalarıyla `config.example.yaml` içindedir. En önemlileri:

| Alan | Varsayılan | Not |
|---|---|---|
| `strategy.markets` | ETH, SOL | Piyasa başına `target_notional`, `order_notional`, `leverage`, `min_edge_bps` ayrı ayarlanabilir. BTC, HYPE ve RWA örnekleri yorum satırı olarak duruyor. |
| `strategy.target_notional` | 150 | Döngü tepesindeki pozisyon (USD, bacak başına). |
| `strategy.order_notional` | 50 | Maker clip. Lighter derinliğine göre seçin (`farmer scan` → `L ToB $`, `RT@$`). |
| `strategy.leverage` | 3 | İki borsada ayarlanır. Düşük tutun. |
| `strategy.min_edge_bps` | 0.5 | Hedge sonrası hedeflenen minimum kâr. **Negatif** değer, hacim için küçük bir maliyeti kabul etmek demektir; daha çok dolum olur. |
| `strategy.max_slippage_bps` | 8 | Hedge IOC emrinin en kötü fiyatı. |
| `strategy.hold_seconds` / `hold_through_funding` | 0 / false | Carry için pozisyonu funding ödemesine kadar tutmak isterseniz `hold_through_funding: true`. |
| `strategy.churn_floor_frac` | 0 | Kapanışta hedefin bu oranı açık kalır. Örneğin 0.5, pozisyonun yarısını hep tutar, yarısıyla hacim üretir. |
| `strategy.direction_mode` | auto | `long_maker` / `short_maker` yönü sabitler. |
| `risk.max_unhedged_notional` | 40 | Bu USD delta 1 sn'den uzun sürerse kotasyon durur, hedge 2x kayma ile gönderilir. |
| `risk.max_total_notional` | 350 | Borsa başına toplam pozisyon sınırı. En az piyasa sayısı × hedef + bir clip olmalı. |
| `risk.max_daily_loss` | 15 | Aşılırsa bot durur (halt). Yeniden başlatana kadar kotasyon vermez. |
| `risk.max_consecutive_errors` / `error_halt_cooldown_s` | 20 / 300 | Bir borsada art arda bu kadar kalıcı hata devre kesiciyi tetikler. Bu süre boyunca hata gelmezse halt kendiliğinden kalkar (0 = kalkmaz). |
| `risk.max_margin_ratio` / `reduce_margin_ratio` | 0.25 / 0.4 | Bakım marjı / özsermaye. Sırasıyla yeni pozisyon açmayı durdurur / botu durdurup tüm pozisyonları taker ile kapatır. |
| `risk.min_free_collateral_mult` | 1.5 | Yeni pozisyon için her borsada serbest teminat >= clip / kaldıraç × bu değer. |
| `risk.max_equity_divergence_frac` | 0.3 | İki borsanın özsermayesi bu oranda ayrışırsa uyarı (teminatı dengeleyin). |
| `risk.close_on_exit` | false | `true`: kapanışta iki bacak da taker ile kapatılır. |
| `paper.initial_balance` | 200 | Paper modunda borsa başına başlangıç bakiyesi. |
| `arcus.dead_mans_switch_s` | 60 | Bot ölürse Arcus emirleri bu kadar saniye sonra iptal edilir. |
| `lighter.max_requests_per_minute` / `read_reserve` | 55 / 25 | Standard 60/dk limitini korur. |

Gizli bilgiler (`ARCUS_*`, `LIGHTER_*`, `TELEGRAM_*`) **yalnızca** ortamdan veya `.env` dosyasından okunur. YAML'a
yazılmaz.

## İzleme ve Telegram

- `monitoring.report_interval_s` aralığıyla (varsayılan 60 sn) loglara bir rapor yazılır. Raporda şunlar bulunur:
  - borsa başına 1 sa / 24 sa / toplam hacim (maker/taker),
  - ücretler,
  - funding,
  - PnL tahmini,
  - bakiye ve marj oranı,
  - çift başına faz, yön, pozisyonlar, delta, kotasyon, carry APR, baz ve duraklama sebebi.
- `.env` içinde `TELEGRAM_BOT_TOKEN` ve `TELEGRAM_CHAT_ID` doluysa iki tür bildirim gelir:
  - önemli olaylar: başlatma, halt, acil hedge, close_all;
  - her `notify_every` raporda bir özet (varsayılan saatlik).
- `farmer stats` geçmişi, `farmer status` anlık hesap durumunu gösterir.
- Tüm dolumlar, funding ödemeleri ve olaylar (faz geçişleri, yön değişimleri) SQLite veritabanına yazılır.
  Saatlik özet satırları sayesinde rapor sorguları veritabanı büyüdükçe yavaşlamaz ve ayrı bir iş parçacığında
  çalışır (event loop'u bloklamaz).
- Funding ödemeleri her `funding_collect_s` saniyede son `funding_lookback_s` (6 sa) için yeniden istenir.
  Websocket'in kaçırdığı bir saat sonradan tamamlanır; tekrar gelen satırlar veritabanında elenir.

Uzun süreli çalıştırma için bir süreç yöneticisi kullanın. systemd örneği:

```ini
[Service]
WorkingDirectory=/opt/farmer
ExecStart=/opt/farmer/.venv/bin/farmer run --live --yes
Restart=on-failure
RestartSec=30
KillSignal=SIGTERM
TimeoutStopSec=180
```

## Sorun giderme ve hata kodları

| Belirti / kod | Anlamı | Ne yapmalı |
|---|---|---|
| Lighter `20558` / "restricted jurisdiction" | Coğrafi kısıtlama | Botu izin verilen bölgeden çalıştırın. Paper ve scan yine çalışır. |
| Lighter `52002` | Standard hesap işlem limiti | Bot yeni pozisyon açmayı bir süre durdurur. Hacmi azaltın veya bekleyin. |
| Lighter `23000` / HTTP 429 | Dakikada 60 istek aşıldı | Bot 60 sn bekler. Aynı IP/adreste başka araç çalıştırmayın. |
| Lighter `21108` / `21109` / `21110` | API anahtarı kayıtlı değil / yanlış yuva | `farmer keygen lighter` çalıştırın veya `LIGHTER_API_KEY_INDEX` değerini kontrol edin. |
| Lighter `21100` | Hesap yok | Önce para yatırın, `LIGHTER_ACCOUNT_INDEX` değerini kontrol edin. |
| Lighter `21120` | Geçersiz imza | Yanlış `chain_id` veya anahtar. |
| Lighter `21104` | Nonce hatası | Bot nonce'u yeniden eşitler. Aynı anahtarı iki süreç kullanıyor olabilir. |
| Lighter `21739` / `21301` / `21507` / `21508` | Teminat yetersiz / marj altı | Teminat ekleyin, boyutu veya kaldıracı düşürün. |
| Lighter `21733` / `21734` | Fiyat mark'tan çok uzak | `max_slippage_bps` değerini düşürün. |
| Arcus HTTP 403 "address not on access whitelist" | Beyaz listede değil | Erişim veya davet kodu alın, `ARCUS_ADDRESS` değerini kontrol edin. |
| Arcus API anahtarı pasif / başka alt hesap için | `connect()` hızlıca hata verir | `farmer keygen arcus --account-index N` ile doğru alt hesaba anahtar oluşturun. |
| Arcus `POST_ONLY_WOULD_CROSS` | Post-only emir karşı tarafa çarptı | Normaldir. Bot bir sonraki adımda yeniden fiyatlar. |
| Arcus `UNDERCOLLATERALIZED` | Teminat yetersiz | Teminat ekleyin veya `order_notional` / `leverage` değerini düşürün. |
| Arcus `OPEN_INTEREST_CAP_EXCEEDED` | Piyasanın OI tavanı dolu | O piyasayı geçici olarak çıkarın. |
| Arcus `REDUCE_ONLY_WOULD_INCREASE` | Pozisyon uyumsuzluğu | Bot reconcile eder. Sürerse `farmer status` ile kontrol edin. |
| Log: `basis warm-up` / `waiting for books` | Normal başlangıç | ~30 sn bekleyin. Sürerse ağ veya websocket bağlantısını kontrol edin. |
| Log: `no edge-compliant quote` | Defter `min_edge_bps` için yeterince iyi değil | Normaldir. Daha çok dolum için `min_edge_bps` değerini düşürün (negatif olabilir). |
| Log: `market closed (trading hours)` | RWA piyasası saat dışında | Normaldir. Bot RTH'de kotasyon verir. |
| Log: `HALTED` | Günlük zarar, devre kesici veya hedge başarısız | Devre kesici halt'ı hatalar kesilince kendiliğinden kalkar (`RESUMED automatically`). Diğerlerinde sebebi giderin, `farmer status` ile pozisyonları kontrol edin, botu yeniden başlatın. |
| Log: `positions not managed by any pair` | Hiçbir çiftin yönetmediği pozisyon var (config dışı, eşlenemeyen ya da reduce-only piyasa) | Config'teki bir piyasaysa `farmer close-all`, değilse borsada elle kapatın. |
| Lighter `market ... is reduce-only` | Lighter piyasayı `force_reduce_only` yaptı | Bot o piyasada yeni pozisyon açmaz, mevcut pozisyonu reduce-only emirlerle kapatabilir. `farmer close-all` kullanın. |
| `another farmer process is running` | Kilit dosyası (`data/farmer*.lock`) | Aynı veritabanında ikinci süreç çalıştırılamaz. Önce diğerini durdurun. |

## Geliştirici notları

- Mimari: `docs/ARCHITECTURE.md`. Strateji yalnızca `exchanges/base.py` içindeki `Exchange` arayüzüyle konuşur.
- Testler ağ kullanmaz:
  ```bash
  .venv/bin/pytest -q
  ```
- Canlı, salt okunur testleri açmak için:
  ```bash
  FARMER_LIVE_TESTS=1 .venv/bin/pytest -q -m live
  ```
- Lint (kurallar `pyproject.toml` içinde; `ruff` dev bağımlılıklarıyla kurulur):
  ```bash
  .venv/bin/ruff check src tests
  ```
- Fiyat ve miktarların hepsi `Decimal` ile tutulur. Funding oranları saatlik kesirdir, pozisyonlar işaretlidir.
