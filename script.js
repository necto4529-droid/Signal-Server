    window.OneSignalDeferred = window.OneSignalDeferred || [];
    OneSignalDeferred.push(async function(OneSignal) {
      await OneSignal.init({ appId: "c5b0ecd0-3e67-47a0-823d-771a7c4de3be" });
    });
(function(){
'use strict';

const SIGNAL_URL='wss://signal-server-aipd.onrender.com';
const A2_MEM=65536,A2_TIME=3,A2_PAR=1;
const QUICK_EMOJIS=['❤️','👍','😂','🔥','👏','😮','😢','➕'];
const EXTENDED_EMOJIS=['❤️‍🔥','😁','👎','🥴','👌','😭','😱','💋','🤝','🥰','🤔','🤯','😡','🎉','🤩','🤮','💩','🙏','🕊️','🤡','🥱','😍','🐳','🌚','🌭','💯','⚡','🍌','🏆','💔','😑','🍓','🍾','🖕','😈','😴','🤓','👻','👨‍💻','👀','🎃','🙈','😇','✍️','🤗','🫡','💅','🤪','🗿','🆒','💘','🦄','😘','💊','😎','👾','🤨','🙉','🥺','☠️','💦','🫂','💪','🤧'];
const WF_BARS=30,PREV_WF_BARS=25,MAX_VOICE_SEC=300,MAX_REACTIONS_PER_MSG=3;
const MAX_FILE_SIZE=500*1024*1024,CHUNK_SIZE=256*1024,STORE_BATCH=3;

const ENC=new TextEncoder(),DEC=new TextDecoder();
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmtTime=ts=>{if(!ts)return'';const d=new Date(ts),n=new Date();return d.toDateString()===n.toDateString()?d.toLocaleTimeString('ru',{hour:'2-digit',minute:'2-digit'}):d.toLocaleDateString('ru',{day:'numeric',month:'short'});};
const uid=()=>'m_'+Date.now()+'_'+Math.random().toString(36).slice(2,6);
const fmtDur=s=>`${Math.floor(s/60)}:${String(s%60).padStart(2,'0')}`;

// ── TOAST ──
function toast(msg,tp=''){const el=$('toast');el.textContent=msg;el.className='toast show'+(tp?' '+tp:'');clearTimeout(el._t);el._t=setTimeout(()=>el.classList.remove('show'),2800);
  // Тактильная обратная связь по типу уведомления
  if(tp==='err')_vib('notificationError');
  else if(tp==='warn')_vib('notificationWarning');
  else _vib('tick');
}
function showProcessingToast(msg){const el=$('toast');el.textContent=msg;el.className='toast processing show';clearTimeout(el._t);}
function showQueueToast(msg){const el=$('toast');el.textContent=msg;el.className='toast queue-progress show';clearTimeout(el._t);}
function hideProgressToast(){const el=$('toast');el.classList.remove('show','processing','queue-progress');}

// ── UTILS ──
function arrayBufferToBase64(buffer){const bytes=new Uint8Array(buffer);const STEP=32768;let binary='';for(let i=0;i<bytes.length;i+=STEP)binary+=String.fromCharCode.apply(null,bytes.subarray(i,Math.min(i+STEP,bytes.length)));return btoa(binary);}
function base64ToArrayBuffer(base64){const binary=atob(base64);const bytes=new Uint8Array(binary.length);for(let i=0;i<binary.length;i++)bytes[i]=binary.charCodeAt(i);return bytes.buffer;}
function payloadToB64(buf){return arrayBufferToBase64(buf instanceof ArrayBuffer?buf:buf.buffer||buf);}
function readFileAsArrayBuffer(file){return new Promise((res,rej)=>{const r=new FileReader();r.onload=()=>res(r.result);r.onerror=rej;r.readAsArrayBuffer(file);});}
// ИСПРАВЛЕНИЕ 2: читаем файл сразу как постоянный data: URL (не blob:)
function readFileAsDataURL(file){return new Promise((res,rej)=>{const r=new FileReader();r.onload=()=>res(r.result);r.onerror=rej;r.readAsDataURL(file);});}
function getFileIcon(mt){mt=mt||'';if(mt.startsWith('video/'))return'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z" /> <path d="M14 2v5a1 1 0 0 0 1 1h5" /> <path d="M15.033 13.44a.647.647 0 0 1 0 1.12l-4.065 2.352a.645.645 0 0 1-.968-.56v-4.704a.645.645 0 0 1 .967-.56z" /> </svg>';if(mt.startsWith('audio/'))return'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M4 6.835V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.706.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2h-.343" /> <path d="M14 2v5a1 1 0 0 0 1 1h5" /> <path d="M2 19a2 2 0 0 1 4 0v1a2 2 0 0 1-4 0v-4a6 6 0 0 1 12 0v4a2 2 0 0 1-4 0v-1a2 2 0 0 1 4 0" /> </svg>';if(mt.includes('pdf'))return'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z" /> <path d="M14 2v5a1 1 0 0 0 1 1h5" /> <path d="M10 9H8" /> <path d="M16 13H8" /> <path d="M16 17H8" /> </svg>';if(mt.includes('zip')||mt.includes('rar')||mt.includes('7z'))return'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <rect width="20" height="5" x="2" y="3" rx="1" /> <path d="M4 8v11a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8" /> <path d="M10 12h4" /> </svg>';if(mt.includes('text')||mt.includes('doc'))return'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z" /> <path d="M14 2v5a1 1 0 0 0 1 1h5" /> <path d="M10 9H8" /> <path d="M16 13H8" /> <path d="M16 17H8" /> </svg>';return'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z" /> </svg>';}
function formatSize(bytes){if(bytes<1024)return bytes+' B';if(bytes<1048576)return(bytes/1024).toFixed(1)+' KB';if(bytes<1073741824)return(bytes/1048576).toFixed(1)+' MB';return(bytes/1073741824).toFixed(2)+' GB';}
function downloadFile(dataUrl,fileName){const a=document.createElement('a');a.href=dataUrl;a.download=fileName;document.body.appendChild(a);a.click();document.body.removeChild(a);}

// ══════════════════════════════════════════════════════════════
// MEDIAN.CO BRIDGE — определение и помощники для скачивания
// В Median webview navigator.userAgent содержит 'median' (или
// 'gonative' для старых проектов GoNative). Объект window.median
// внедряется нативным кодом и доступен НЕ сразу синхронно при
// первой загрузке страницы, поэтому ждём его появления.
// ══════════════════════════════════════════════════════════════
const IS_MEDIAN=/median|gonative/i.test(navigator.userAgent||'');

function waitForMedianBridge(timeoutMs=1500){
  return new Promise(resolve=>{
    if(window.median&&window.median.share){resolve(window.median);return;}
    if(!IS_MEDIAN){resolve(null);return;}
    const start=Date.now();
    const iv=setInterval(()=>{
      if(window.median&&window.median.share){clearInterval(iv);resolve(window.median);return;}
      if(Date.now()-start>timeoutMs){clearInterval(iv);resolve(window.median||null);}
    },50);
  });
}

// blob/dataURL → data: URI (нужен для median:// и median.share.*)
async function toDataURI(blob){
  return new Promise((res,rej)=>{
    const r=new FileReader();
    r.onload=()=>res(r.result);
    r.onerror=rej;
    r.readAsDataURL(blob);
  });
}

// Вызов median:// через скрытый iframe — самый надёжный способ,
// работает даже если JS Bridge библиотека (window.median) ещё
// не инжектирована или была отключена в Web Overrides.
function callMedianScheme(path,params){
  try{
    const qs=encodeURIComponent(JSON.stringify(params));
    const url=`median://${path}?options=${qs}`;
    const ifr=document.createElement('iframe');
    ifr.style.display='none';
    ifr.src=url;
    document.body.appendChild(ifr);
    setTimeout(()=>{ifr.remove();},1000);
    return true;
  }catch(e){
    console.error('callMedianScheme error',e);
    return false;
  }
}

// Унифицированное скачивание внутри Median-приложения.
// type: 'image' → median.share.downloadImage (сохраняет в галерею)
// type: 'file'  → median.share.downloadFile (сохраняет в Файлы/Downloads)
async function medianDownload(blob,fileName,type){
  const dataUri=await toDataURI(blob);
  const bridge=await waitForMedianBridge();
  if(bridge&&bridge.share){
    try{
      if(type==='image'&&typeof bridge.share.downloadImage==='function'){
        await bridge.share.downloadImage({url:dataUri});
        return true;
      }
      if(typeof bridge.share.downloadFile==='function'){
        await bridge.share.downloadFile({url:dataUri,filename:fileName,open:false});
        return true;
      }
    }catch(e){
      console.warn('median.share bridge call failed, falling back to scheme',e);
    }
  }
  // Фоллбэк: вызов через median:// протокол (работает без window.median)
  if(type==='image'){
    return callMedianScheme('nativebridge/share/downloadImage',{url:dataUri});
  }
  return callMedianScheme('nativebridge/share/downloadFile',{url:dataUri,filename:fileName,open:false});
}

// ── Сохранить медиа (фото/видео) в галерею ──
async function saveToGallery(dataUrlOrBlobId,mimeType,fileName,isBlob){
  try{
    let blob;
    if(isBlob){
      const buf=await loadBlob(dataUrlOrBlobId);
      if(!buf){toast('Файл недоступен','err');return;}
      blob=new Blob([buf],{type:mimeType||'image/jpeg'});
    }else{
      // dataUrl → Blob
      const res=await fetch(dataUrlOrBlobId);
      blob=await res.blob();
    }
    const isVideo=mimeType&&mimeType.startsWith('video/');
    const ext=isVideo?(fileName?.split('.').pop()||'mp4'):(fileName?.split('.').pop()||'jpg');
    const name=fileName||('kchat_'+(isVideo?'video':'photo')+'_'+Date.now()+'.'+ext);

    // ── Внутри приложения Median.co (Android/iOS) ──
    if(IS_MEDIAN){
      try{
        if(isVideo){
          // downloadImage предназначен только для изображений (Photos/Gallery).
          // Для видео используем downloadFile — на Android с разрешением
          // "Downloads Folder" файл сохранится в Загрузки и появится в галерее
          // после индексации медиа-сканером.
          const ok=await medianDownload(blob,name,'file');
          if(ok){toast('Видео сохранено');return;}
        }else{
          const ok=await medianDownload(blob,name,'image');
          if(ok){toast('Изображение сохранено');return;}
        }
      }catch(e){
        console.error('Median save error',e);
        // падаем дальше на универсальные фоллбэки
      }
    }

    // Пробуем Web Share API (работает в мобильных браузерах — Safari iOS, Chrome Android)
    if(navigator.canShare&&navigator.canShare({files:[new File([blob],name,{type:blob.type})]})){
      try{
        await navigator.share({files:[new File([blob],name,{type:blob.type})],title:name});
        toast(isVideo?'Видео сохранено':'Изображение сохранено');
        return;
      }catch(e){
        if(e.name==='AbortError')return; // пользователь отменил
        // fallback ниже
      }
    }
    // Fallback: скачивание через <a> (Android Chrome сохраняет в Downloads)
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a');a.href=url;a.download=name;
    document.body.appendChild(a);a.click();document.body.removeChild(a);
    setTimeout(()=>URL.revokeObjectURL(url),5000);
    toast(isVideo?'Видео сохранено':'Изображение сохранено');
  }catch(e){
    console.error('saveToGallery error',e);
    toast('Ошибка сохранения','err');
  }
}

// ── Сохранить файл в Downloads ──
async function saveToFiles(dataUrlOrBlobId,mimeType,fileName,isBlob){
  try{
    let blob;
    if(isBlob){
      const buf=await loadBlob(dataUrlOrBlobId);
      if(!buf){toast('Файл недоступен','err');return;}
      blob=new Blob([buf],{type:mimeType||'application/octet-stream'});
    }else{
      const res=await fetch(dataUrlOrBlobId);
      blob=await res.blob();
    }
    const name=fileName||('kchat_file_'+Date.now());

    // ── Внутри приложения Median.co (Android/iOS) ──
    if(IS_MEDIAN){
      try{
        const ok=await medianDownload(blob,name,'file');
        if(ok){toast('Файл сохранён');return;}
      }catch(e){
        console.error('Median save error',e);
        // падаем дальше на универсальные фоллбэки
      }
    }

    // Пробуем File System Access API (Chrome Desktop, Android Chrome 86+)
    if(window.showSaveFilePicker){
      try{
        const ext=name.split('.').pop()||'bin';
        const fh=await window.showSaveFilePicker({suggestedName:name,types:[{description:'File',accept:{[blob.type||'application/octet-stream']:['.'+(ext)]}}]});
        const writable=await fh.createWritable();
        await writable.write(blob);
        await writable.close();
        toast('Файл сохранён');
        return;
      }catch(e){
        if(e.name==='AbortError')return;
        // fallback ниже
      }
    }
    // Fallback: <a download>
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a');a.href=url;a.download=name;
    document.body.appendChild(a);a.click();document.body.removeChild(a);
    setTimeout(()=>URL.revokeObjectURL(url),5000);
    toast('Файл сохранён');
  }catch(e){
    console.error('saveToFiles error',e);
    toast('Ошибка сохранения','err');
  }
}

// ── ID ──
function mkId(){let id=localStorage.getItem('bc_my_id');if(!id){const h=()=>Math.random().toString(36).slice(2,6);id=`bc-${h()}-${h()}`;localStorage.setItem('bc_my_id',id);}return id.toLowerCase();}
const MY_ID=mkId();
// ── ОЧЕРЕДЬ ОТПРАВКИ (OUTBOX) ──
let outbox = JSON.parse(localStorage.getItem('kchat_outbox') || '[]');
function saveOutbox() { localStorage.setItem('kchat_outbox', JSON.stringify(outbox)); }

async function processOutbox() {
    if (!wsUp || outbox.length === 0) return;
    const item = outbox[0];
    try {
        if (item.type === 'msg') {
            await sendSingleMsg(item.text, null, item.msgId);
        } else if (item.type === 'file') {
            const buf = await loadBlob(item.fileId);
            if(buf){
              const fileInfo = {
                blob: new Blob([buf], {type: item.fileInfo.type}),
                name: item.fileInfo.name,
                type: item.fileInfo.type,
                size: item.fileInfo.size
              };
              await sendFileToServer(fileInfo, item.text, item.msgId);
            }
        }
        outbox.shift();
        saveOutbox();
        // Успешно отправлено — убираем иконку ожидания
        _markMsgDelivered(item.target, item.msgId);
        setTimeout(processOutbox, 300);
    } catch (e) {
        console.error('Outbox processing error', e);
        setTimeout(processOutbox, 5000);
    }
}
$('homeMyId').title=MY_ID+' — нажми чтобы скопировать';
window.copyMyId=()=>copyToClipboard(MY_ID).then(()=>toast('ID скопирован ✓'));
async function copyToClipboard(text){try{if(navigator.clipboard?.writeText){await navigator.clipboard.writeText(text);return;}}catch(e){}const ta=document.createElement('textarea');ta.value=text;ta.style.cssText='position:fixed;top:0;left:0;width:1px;height:1px;opacity:0';document.body.appendChild(ta);ta.focus();ta.select();try{document.execCommand('copy');}catch(e){}document.body.removeChild(ta);}

// ── STORAGE ──
const lsGet=(k,fb)=>{try{return JSON.parse(localStorage.getItem(k)||'null')??fb;}catch{return fb;}};
const lsSet=(k,v)=>localStorage.setItem(k,JSON.stringify(v));

// ══════════════════════════════════════════════════════════════
// ХАПТИКА / ВИБРАЦИЯ ──────────────────────────────────────────────
// Внутри Median.co-приложения используем нативный Haptics Plugin:
//   median.haptics.trigger({ style })
// style: impactLight | impactMedium | impactHeavy |
//        notificationSuccess | notificationWarning | notificationError |
//        tick | click | double_click  (последние три — Android-only)
// Если бридж ещё не инжектирован — пробуем median:// схему (как и для
// share/download). В обычном браузере/PWA — Web Vibration API.
// Управляется тогглом "📳 Вибрация" в настройках (bc_haptics_enabled).
// ══════════════════════════════════════════════════════════════
const VIB_FALLBACK_MS={
  impactLight:8, impactMedium:18, impactHeavy:32,
  notificationSuccess:[12,45,12], notificationWarning:[18,70,18], notificationError:[25,70,25,70,25],
  tick:5, click:12, double_click:[10,40,10],
};
function _vib(style='impactLight'){
  if(!lsGet('bc_haptics_enabled',true))return;
  try{
    if(window.median&&window.median.haptics&&typeof window.median.haptics.trigger==='function'){
      window.median.haptics.trigger({style});
      return;
    }
    if(IS_MEDIAN){
      // Бридж может быть ещё не готов сразу после старта — пробуем через
      // median:// схему (тот же механизм что и для share/download)
      callMedianScheme('nativebridge/haptics/trigger',{style});
      return;
    }
  }catch(e){}
  // Веб/PWA fallback — Vibration API (Android Chrome; iOS Safari её не поддерживает)
  if(navigator.vibrate){
    try{navigator.vibrate(VIB_FALLBACK_MS[style]||10);}catch(e){}
  }
}
const DB_NAME='KChatMessages',STORE_NAME='messages';
// IDB version 3: добавлен store 'meta' для chats и других метаданных
const DB_VERSION=3;
let db=null;
let _dbOpenPromise=null;

// ═══════════════════════════════════════════════════════════
// НАДЁЖНЫЙ IDB v3 — исправления:
// 1. Чаты (bc_chats) хранятся в IDB 'meta', а не только в localStorage
// 2. Сообщения хранятся в IDB 'messages' + ls-backup (только текст, без медиа)
// 3. openMessagesDB: проверка живости через objectStoreNames вместо abort()
// 4. saveMsgs: ls-backup очищен от data: URL (только структура) → не переполняет quota
// 5. loadMsgs: умный merge IDB + ls для защиты от любой потери данных
// ═══════════════════════════════════════════════════════════
function openMessagesDB(){
  // Проверяем живость кэшированного соединения через свойство (не через транзакцию!)
  if(db){
    try{
      // objectStoreNames.contains() не создаёт транзакций — безопасная проверка
      if(db.objectStoreNames.contains(STORE_NAME)) return Promise.resolve(db);
    }catch(e){}
    db=null; _dbOpenPromise=null;
  }
  if(_dbOpenPromise) return _dbOpenPromise;
  _dbOpenPromise=new Promise((resolve,reject)=>{
    const req=indexedDB.open(DB_NAME,DB_VERSION);
    req.onerror=()=>{_dbOpenPromise=null;reject(req.error);};
    req.onsuccess=()=>{
      db=req.result;
      _dbOpenPromise=null;
      db.onversionchange=()=>{db.close();db=null;_dbOpenPromise=null;};
      db.onclose=()=>{db=null;_dbOpenPromise=null;};
      resolve(db);
    };
    req.onupgradeneeded=ev=>{
      const d=ev.target.result;
      if(!d.objectStoreNames.contains(STORE_NAME))
        d.createObjectStore(STORE_NAME,{keyPath:'peerId'});
      if(!d.objectStoreNames.contains('blobs'))
        d.createObjectStore('blobs');
      // NEW в v3: хранилище для метаданных (чаты, настройки и т.д.)
      if(!d.objectStoreNames.contains('meta'))
        d.createObjectStore('meta');
    };
  });
  return _dbOpenPromise;
}

// ── Вспомогательные IDB-операции ──
async function _idbGet(peerId){
  const d=await openMessagesDB();
  return new Promise((res,rej)=>{
    const tx=d.transaction(STORE_NAME,'readonly');
    const req=tx.objectStore(STORE_NAME).get(peerId);
    req.onsuccess=()=>res(req.result?req.result.messages:[]);
    req.onerror=()=>rej(req.error);
    tx.onerror=()=>rej(tx.error);
    tx.onabort=()=>rej(new Error('tx aborted'));
  });
}

async function _idbPut(peerId,messages){
  const d=await openMessagesDB();
  return new Promise((res,rej)=>{
    const tx=d.transaction(STORE_NAME,'readwrite');
    const req=tx.objectStore(STORE_NAME).put({peerId,messages});
    req.onerror=()=>rej(req.error);
    tx.oncomplete=()=>res();
    tx.onerror=()=>rej(tx.error);
    tx.onabort=()=>rej(new Error('tx aborted'));
  });
}

async function _idbDelete(peerId){
  const d=await openMessagesDB();
  return new Promise((res,rej)=>{
    const tx=d.transaction(STORE_NAME,'readwrite');
    tx.objectStore(STORE_NAME).delete(peerId);
    tx.oncomplete=()=>res();
    tx.onerror=()=>rej(tx.error);
  });
}

// ── META store (для чатов и других ключ→значение данных) ──
async function _metaGet(key){
  const d=await openMessagesDB();
  return new Promise((res,rej)=>{
    const tx=d.transaction('meta','readonly');
    const req=tx.objectStore('meta').get(key);
    req.onsuccess=()=>res(req.result!==undefined?req.result:null);
    req.onerror=()=>rej(req.error);
    tx.onerror=()=>rej(tx.error);
  });
}

async function _metaPut(key,value){
  const d=await openMessagesDB();
  return new Promise((res,rej)=>{
    const tx=d.transaction('meta','readwrite');
    const req=tx.objectStore('meta').put(value,key);
    req.onerror=()=>rej(req.error);
    tx.oncomplete=()=>res();
    tx.onerror=()=>rej(tx.error);
    tx.onabort=()=>rej(new Error('tx aborted'));
  });
}

// ── Stripped ls-backup: убираем бинарные данные перед записью в localStorage ──
// Это предотвращает QuotaExceededError от фото/видео/голосовых в backup
function _stripBinaryForBackup(msgs){
  return msgs.map(m=>{
    if(!m.media&&!m.voice&&!m.file) return m; // текстовые сообщения — без изменений
    const stripped={...m};
    if(m.media?.data){stripped.media={...m.media};delete stripped.media.data;}
    if(m.voice?.data){stripped.voice={...m.voice};delete stripped.voice.data;}
    if(m.file?.data){stripped.file={...m.file};delete stripped.file.data;}
    return stripped;
  });
}

async function loadMsgs(peerId){
  let idbResult=null,idbOk=false;
  // Попытка 1: IDB (основное хранилище)
  try{ idbResult=await _idbGet(peerId); idbOk=true; }catch(e){ db=null;_dbOpenPromise=null; }
  if(!idbOk){
    // Небольшая пауза и retry
    await new Promise(r=>setTimeout(r,120));
    try{ idbResult=await _idbGet(peerId); idbOk=true; }catch(e){ db=null;_dbOpenPromise=null; }
  }
  const ls=lsGet(`bc_msgs_${peerId}`,null);
  if(idbOk){
    // Защита: если IDB пуст, но ls-backup есть — возвращаем backup
    if((!idbResult||!idbResult.length)&&ls&&ls.length){
      // Пишем backup обратно в IDB чтобы починить рассинхронизацию
      _idbPut(peerId,ls).catch(()=>{});
      return ls;
    }
    return idbResult||[];
  }
  // IDB недоступен — fallback на ls-backup
  return ls&&ls.length?ls:[];
}

async function saveMsgs(peerId,messages){
  // ls-backup: только структура БЕЗ binary data — не переполняет localStorage quota
  const lsLimit=200;
  try{
    const stripped=_stripBinaryForBackup(messages.slice(-lsLimit));
    lsSet(`bc_msgs_${peerId}`,stripped);
  }catch(e){}
  // Основное хранилище — IDB (хранит всё включая binary data)
  for(let i=0;i<3;i++){
    try{ await _idbPut(peerId,messages); return; }catch(e){ db=null;_dbOpenPromise=null; }
    if(i<2) await new Promise(r=>setTimeout(r,i===0?80:200));
  }
  // IDB недоступен — ls-backup уже записан выше
}

async function clearMsgs(peerId){
  localStorage.removeItem(`bc_msgs_${peerId}`);
  for(let i=0;i<2;i++){
    try{ await _idbDelete(peerId); return; }catch(e){ db=null;_dbOpenPromise=null; }
    if(i===0) await new Promise(r=>setTimeout(r,100));
  }
}

// ══════════════════════════════════════════════════════
// ── BLOB STORAGE (медиа/файлы/голосовые в IDB 'blobs') ──
// ══════════════════════════════════════════════════════
function _blobStore(mode){
  return openMessagesDB().then(d=>d.transaction('blobs',mode).objectStore('blobs'));
}

async function saveBlob(blobId,buffer){
  for(let i=0;i<2;i++){
    try{
      const d=await openMessagesDB();
      await new Promise((res,rej)=>{
        const tx=d.transaction('blobs','readwrite');
        tx.objectStore('blobs').put(buffer,blobId);
        tx.oncomplete=()=>res();
        tx.onerror=()=>rej(tx.error);
      });
      return;
    }catch(e){db=null;_dbOpenPromise=null;if(i===0)await new Promise(r=>setTimeout(r,80));}
  }
  console.warn('[blobs] saveBlob failed for',blobId);
}

async function loadBlob(blobId){
  for(let i=0;i<2;i++){
    try{
      const d=await openMessagesDB();
      const buf=await new Promise((res,rej)=>{
        const tx=d.transaction('blobs','readonly');
        const req=tx.objectStore('blobs').get(blobId);
        req.onsuccess=()=>res(req.result);
        req.onerror=()=>rej(req.error);
      });
      return buf||null;
    }catch(e){db=null;_dbOpenPromise=null;if(i===0)await new Promise(r=>setTimeout(r,80));}
  }
  return null;
}

async function deleteBlob(blobId){
  try{
    const d=await openMessagesDB();
    await new Promise((res,rej)=>{
      const tx=d.transaction('blobs','readwrite');
      tx.objectStore('blobs').delete(blobId);
      tx.oncomplete=()=>res();
      tx.onerror=()=>rej(tx.error);
    });
  }catch(e){console.warn('[blobs] deleteBlob failed',blobId,e);}
}

async function _blobToObjectURL(blobId,mimeType){
  const buf=await loadBlob(blobId);
  if(!buf)return null;
  const blob=new Blob([buf],{type:mimeType||'application/octet-stream'});
  return URL.createObjectURL(blob);
}
const loadContacts=()=>lsGet('bc_contacts',[]);
const saveContacts=a=>lsSet('bc_contacts',a);
// ── CHATS: IDB 'meta' — основное хранилище; localStorage — backup ──
// ВАЖНО: раньше чаты хранились ТОЛЬКО в localStorage → терялись при очистке браузера.
// Теперь IDB — главное, ls — быстрый кэш для первого рендера.
let _chatsCache=null;
const CHATS_META_KEY='bc_chats_v3';

async function loadChats(){
  if(_chatsCache!==null) return _chatsCache;
  // 1. Быстрый старт из localStorage (синхронно, чтобы список чатов появился сразу)
  const lsChats=lsGet('bc_chats',null);
  // 2. IDB — основное хранилище (async)
  let idbChats=null;
  try{ idbChats=await _metaGet(CHATS_META_KEY); }catch(e){}
  if(idbChats&&idbChats.length){
    // IDB есть — это самая свежая версия (могла быть обновлена в фоне)
    _chatsCache=idbChats;
  }else if(lsChats&&lsChats.length){
    // IDB пуст (первый запуск v3 или IDB очищен) — берём из ls и пишем в IDB
    _chatsCache=lsChats;
    _metaPut(CHATS_META_KEY,lsChats).catch(()=>{});
  }else{
    _chatsCache=[];
  }
  return _chatsCache;
}

async function saveChats(chats){
  _chatsCache=chats;
  // Пишем в оба хранилища параллельно
  try{ lsSet('bc_chats',chats); }catch(e){}
  try{ await _metaPut(CHATS_META_KEY,chats); }catch(e){
    // Retry один раз
    await new Promise(r=>setTimeout(r,80));
    try{ await _metaPut(CHATS_META_KEY,chats); }catch(e2){}
  }
}
async function initFavoritesChat(){let chats=await loadChats();if(!chats.find(c=>c.peerId===MY_ID)){chats.push({peerId:MY_ID,peerName:'⭐ Избранное',avatar:'⭐',lastMsg:null,lastMsgTime:null,unread:0});await saveChats(chats);}}
async function warmAllChatKeys(){
  const chats=await loadChats();
  for(const chat of chats){
    if(chat.peerId===MY_ID)continue;
    await getKey(chat.peerId).catch(()=>{});
  }
}
const chatLocks=new Map();
async function withChatLock(pid,fn){while(chatLocks.get(pid))await chatLocks.get(pid);let resolveLock;const lockPromise=new Promise(res=>{resolveLock=res;});chatLocks.set(pid,lockPromise);try{return await fn();}finally{chatLocks.delete(pid);resolveLock();}}
async function upsertMsg(pid,m){return withChatLock(pid,async()=>{let a=await loadMsgs(pid);const i=a.findIndex(x=>x.id===m.id);if(i!==-1)a[i]={...a[i],...m};else{a.push(m);if(a.length>500)a=a.slice(-500);}await saveMsgs(pid,a);});}

// ── CRYPTO ──
const keyCache={};
async function deriveKey(pwd,peerId){const saltStr=[MY_ID,peerId].sort().join(':');const saltBuf=new Uint8Array(32);const encoded=ENC.encode(saltStr);saltBuf.set(encoded.slice(0,32));const result=await argon2.hash({pass:pwd,salt:saltBuf,time:A2_TIME,mem:A2_MEM,parallelism:A2_PAR,hashLen:32,type:argon2.ArgonType.Argon2id});const key=await crypto.subtle.importKey('raw',result.hash,{name:'AES-GCM'},true,['encrypt','decrypt']);result.hash.fill(0);return key;}
async function persistKey(pid,key){const raw=await crypto.subtle.exportKey('raw',key);localStorage.setItem(`bc_secret_${pid}`,btoa(String.fromCharCode(...new Uint8Array(raw))));}
async function loadPersistedKey(pid){const b64=localStorage.getItem(`bc_secret_${pid}`);if(!b64)return null;const raw=Uint8Array.from(atob(b64),c=>c.charCodeAt(0));return crypto.subtle.importKey('raw',raw,{name:'AES-GCM'},false,['encrypt','decrypt']);}
async function getKey(pid){if(keyCache[pid])return keyCache[pid];const k=await loadPersistedKey(pid);if(k)keyCache[pid]=k;return k||null;}
function warmKey(pid){getKey(pid).then(k=>{if(!k){const pwd=localStorage.getItem(`bc_pwd_${pid}`)||sessionStorage.getItem(`bc_pwd_${pid}`);if(pwd)deriveKey(pwd,pid).then(async key=>{keyCache[pid]=key;await persistKey(pid,key);updateSendBtn();}).catch(()=>{});}else updateSendBtn();});}
async function encData(data,key){const iv=crypto.getRandomValues(new Uint8Array(12));const ct=await crypto.subtle.encrypt({name:'AES-GCM',iv},key,data);const out=new Uint8Array(12+ct.byteLength);out.set(iv);out.set(new Uint8Array(ct),12);return out.buffer;}
async function decData(buf,key){const d=new Uint8Array(buf);return crypto.subtle.decrypt({name:'AES-GCM',iv:d.slice(0,12)},key,d.slice(12));}
let _kmResolve=null,_kmReject=null,_kmPid=null;
function promptKey(pid){return new Promise((res,rej)=>{_kmResolve=res;_kmReject=rej;_kmPid=pid;loadChats().then(chats=>{const chat=chats.find(c=>c.peerId===pid)||{};$('keyModalTitle').innerHTML=`<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="m15.5 7.5 2.3 2.3a1 1 0 0 0 1.4 0l2.1-2.1a1 1 0 0 0 0-1.4L19 4" /> <path d="m21 2-9.6 9.6" /> <circle cx="7.5" cy="15.5" r="5.5" /> </svg> Ключ для «${chat.peerName||pid}»`;});$('keyModalPwd').value='';$('keySpinner').style.display='none';$('keyModalBtn').disabled=false;$('keyModalBtn').textContent='Продолжить';$('keyModalOverlay').classList.add('open');setTimeout(()=>$('keyModalPwd').focus(),300);});}
$('keyModalPwd')?.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();submitKeyModal();}});
window.submitKeyModal=async function(){const pwd=$('keyModalPwd').value;if(pwd.length<16){toast('Пароль должен быть не менее 16 символов','err');return;}$('keySpinner').style.display='flex';$('keyModalBtn').disabled=true;$('keyModalBtn').textContent='Вычисление…';try{const key=await deriveKey(pwd,_kmPid);keyCache[_kmPid]=key;if($('keyModalRemember').checked){localStorage.setItem(`bc_pwd_${_kmPid}`,pwd);await persistKey(_kmPid,key);}else sessionStorage.setItem(`bc_pwd_${_kmPid}`,pwd);$('keyModalOverlay').classList.remove('open');if(_kmResolve){_kmResolve(key);_kmResolve=null;}updateSendBtn();}catch(e){toast('Ошибка вычисления ключа','err');$('keySpinner').style.display='none';$('keyModalBtn').disabled=false;$('keyModalBtn').textContent='Продолжить';}};
window.cancelKeyModal=function(){$('keyModalOverlay').classList.remove('open');if(_kmReject){_kmReject(new Error('cancelled'));_kmReject=null;}};
async function ensureKey(pid){let key=await getKey(pid);if(key)return key;return promptKey(pid);}

// ══════════════════════════════════════════════════════
// ── АНИМИРОВАННЫЕ СТАТУСЫ (Telegram-style, heartbeat) ──
// ══════════════════════════════════════════════════════
// activityType: 'typing'|'recording'|'sending_voice'|'sending_file'|'choosing_sticker'|null
const ACTIVITY_LABELS={
  typing:'печатает',
  recording:'записывает голосовое',
  sending_voice:'отправляет голосовое',
  sending_file:'отправляет файл',
  choosing_sticker:'выбирает стикер',
};
let _peerActivityInterval=null;
let _peerActivityDots=1;
let _peerActivityType=null;
// Таймер сброса — сбрасывается при каждом heartbeat (5с > интервал 3с)
let _activityClearTimer=null;

function startPeerActivity(type){
  // Всегда сбрасываем таймер автосброса при любом входящем heartbeat-сигнале
  clearTimeout(_activityClearTimer);
  // Автосброс через 5с если heartbeat прекратились (интервал отправки 3с + 2с запас)
  _activityClearTimer=setTimeout(()=>{stopPeerActivity();updateBar();},5000);

  if(_peerActivityType===type) return; // уже показываем этот тип, таймер уже обновлён

  // Новый тип — перезапускаем анимацию
  if(_peerActivityInterval){clearInterval(_peerActivityInterval);_peerActivityInterval=null;}
  _peerActivityType=type;
  _peerActivityDots=1;
  const base=ACTIVITY_LABELS[type]||type;
  const el=$('peerStatus');
  function render(){
    if(!el)return;
    el.textContent=base+'.'.repeat(_peerActivityDots);
    el.className='chat-peer-status activity';
    _peerActivityDots=_peerActivityDots>=3?1:_peerActivityDots+1;
  }
  render();
  _peerActivityInterval=setInterval(render,500);
}

function stopPeerActivity(){
  if(_peerActivityInterval){clearInterval(_peerActivityInterval);_peerActivityInterval=null;}
  clearTimeout(_activityClearTimer);_activityClearTimer=null;
  _peerActivityType=null;
  _peerActivityDots=1;
}

// ── WEBSOCKET ──
let ws=null,wsUp=false,wsRetry=0,pingInterval=null;
let lastHiddenTime=0,ignoreNextVisibilityReturn=false;
const WS_DELAYS=[2000,3000,5000,8000,15000,30000];

// ── ОПТИМИЗАЦИЯ 1: Network Quality Monitor ──────────────────────────────────
// Читает navigator.connection и определяет тип сети: slow-2g / 2g / 3g / 4g
// Автоматически обновляется при смене сети.
const _netQuality = {
  type: '4g',
  update() {
    try {
      const conn = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
      if (conn) {
        const ect = conn.effectiveType || '4g';
        this.type = ect; // 'slow-2g' | '2g' | '3g' | '4g'
      }
    } catch(e) {}
  },
  isSlow() { return this.type === 'slow-2g' || this.type === '2g'; },
  multiplier() {
    if (this.type === 'slow-2g') return 2.0;
    if (this.type === '2g')      return 1.5;
    if (this.type === '3g')      return 1.2;
    return 1.0;
  }
};
_netQuality.update();
try {
  const _nconn = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
  if (_nconn) _nconn.addEventListener('change', () => _netQuality.update());
} catch(e) {}

// ── ОПТИМИЗАЦИЯ 2: Adaptive Reconnect с Jitter ──────────────────────────────
// Задержка учитывает тип сети + ±30% jitter
function wsReconnectDelay(attempt) {
  const base = WS_DELAYS[Math.min(attempt, WS_DELAYS.length - 1)];
  const scaled = Math.round(base * _netQuality.multiplier());
  const jitter = scaled * 0.3;
  return Math.round(scaled - jitter + Math.random() * jitter * 2);
}

// ── ОПТИМИЗАЦИЯ 3: Persistent Outbox ────────────────────────────────────────
// Дурабельная очередь исходящих сообщений в localStorage (bc_outbox_v2).
// При отсутствии соединения сообщение сохраняется в очередь и
// автоматически отправляется при reconnect. Переживает reload страницы.
const OUTBOX_KEY = 'bc_outbox_v2';
const OUTBOX_TTL = 5 * 60 * 1000; // 5 минут
function _outboxLoad() {
  const now = Date.now();
  const items = lsGet(OUTBOX_KEY, []);
  return items.filter(x => x && x.ts && (now - x.ts) < OUTBOX_TTL);
}
function _outboxSave(items) {
  try { lsSet(OUTBOX_KEY, items); } catch(e) {}
}
function _outboxPush(item) {
  const items = _outboxLoad();
  if (!items.some(x => x.msgId === item.msgId)) {
    items.push({ ...item, ts: Date.now() });
    _outboxSave(items);
  }
}
function _outboxRemove(msgId) {
  const items = _outboxLoad().filter(x => x.msgId !== msgId);
  _outboxSave(items);
}
async function _drainOutbox() {
  if (!wsUp) return;
  const items = _outboxLoad();
  if (!items.length) return;
  const remaining = [];
  for (const item of items) {
    if (!wsUp) { remaining.push(item); continue; }
    try {
      const key = await getKey(item.peerId);
      if (!key) { remaining.push(item); continue; }
      const env = { type: 'msg', id: item.msgId, text: item.text, ts: item.ts, replyTo: item.replyTo || null, forwarded: false };
      const enc = await encData(ENC.encode(JSON.stringify(env)), key);
      wsSend({ type: 'send-msg', target: item.peerId, msgId: item.msgId, payload: payloadToB64(enc) });
      _markMsgSent(item.peerId, item.msgId);
    } catch(e) { remaining.push(item); }
  }
  _outboxSave(remaining);
}

// ── ОПТИМИЗАЦИЯ 5: wsSend с буферизацией ────────────────────────────────────
// Критические пакеты не дропаются при недоступном WS, а буферизуются.
window._wsSendBuffer = window._wsSendBuffer || [];
const _WS_BUFFER_TYPES = new Set(['send-msg','ack-msg','chat-read','ack-event','voice-listened','vn-watched']);
const _WS_BUFFER_MAX = 100;

// ── ОПТИМИЗАЦИЯ 9: Индикатор «ожидает отправки» ─────────────────────────────
function _markMsgPending(peerId, msgId) {
  if (!peerId || !msgId) return;
  const row = document.querySelector(`.msg-row[data-msgid="${msgId}"]`);
  if (!row) return;
  const tk = row.querySelector('.msg-ticks');
  if (tk) {
    tk.className = 'msg-ticks pending';
    tk.innerHTML = '<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>';
    tk.title = 'Ожидает отправки';
  }
}
function _markMsgSent(peerId, msgId) {
  if (!peerId || !msgId) return;
  const row = document.querySelector(`.msg-row[data-msgid="${msgId}"]`);
  if (!row) return;
  const tk = row.querySelector('.msg-ticks');
  if (tk) {
    tk.className = 'msg-ticks sent';
    tk.innerHTML = '<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>';
    tk.title = 'Отправлено на сервер';
  }
}
function _markMsgDelivered(peerId, msgId) {
  if (!peerId || !msgId) return;
  const row = document.querySelector(`.msg-row[data-msgid="${msgId}"]`);
  if (!row) return;
  const tk = row.querySelector('.msg-ticks');
  if (tk) {
    tk.className = 'msg-ticks delivered';
    tk.innerHTML = '<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/><polyline points="20 12 9 23 4 18"/></svg>';
    tk.title = 'Доставлено';
  }
}
// ── PENDING ACKS — задержанные acks до открытия чата ──
// Храним не только в памяти, но и в localStorage, чтобы reconnect /
// уход со страницы / leaveChat не ломали read-receipts.
// Ключ: peerId, Значение: [{msgId, eventId, ts}]
const PENDING_ACKS_LS_KEY='bc_pending_read_acks_v1';
function _loadPendingAcksMap(){
  const raw=lsGet(PENDING_ACKS_LS_KEY,{});
  const map=new Map();
  if(raw&&typeof raw==='object'){
    for(const [pid,list] of Object.entries(raw)){
      const clean=(Array.isArray(list)?list:[]).filter(x=>x&&x.msgId).map(x=>({msgId:x.msgId,eventId:x.eventId||null,ts:x.ts||Date.now()}));
      if(clean.length)map.set(pid.toLowerCase(),clean);
    }
  }
  return map;
}
function _savePendingAcks(){
  const obj={};
  for(const [pid,list] of _pendingAcks.entries()) if(list&&list.length) obj[pid]=list;
  try{lsSet(PENDING_ACKS_LS_KEY,obj);}catch(e){}
}
const _pendingAcks = _loadPendingAcksMap();
function _queuePendingAck(pid,msgId,eventId){
  pid=(pid||'').toLowerCase();
  if(!pid||!msgId)return;
  const list=_pendingAcks.get(pid)||[];
  const existing=list.find(x=>x.msgId===msgId);
  if(existing){
    if(eventId&&!existing.eventId)existing.eventId=eventId;
  }else{
    list.push({msgId,eventId:eventId||null,ts:Date.now()});
  }
  _pendingAcks.set(pid,list);
  _savePendingAcks();
}
function _flushPendingAcks(pid){
  pid=(pid||'').toLowerCase();
  const acks=_pendingAcks.get(pid);
  if(!acks||!acks.length)return false;
  if(!wsUp||!ws||ws.readyState!==WebSocket.OPEN)return false;
  const sentMsgIds=new Set();
  const sentEventIds=new Set();
  for(const {msgId,eventId} of acks){
    if(msgId&&!sentMsgIds.has(msgId)){
      wsSend({type:'ack-msg',msgId});
      sentMsgIds.add(msgId);
    }
    if(eventId&&!sentEventIds.has(eventId)){
      wsSend({type:'ack-event',eventId});
      sentEventIds.add(eventId);
    }
  }
  _pendingAcks.delete(pid);
  _savePendingAcks();
  return true;
}
function _flushAllPendingAcks(){
  for(const pid of [..._pendingAcks.keys()]) _flushPendingAcks(pid);
}

let _wsLock = false;
let _lastPong = Date.now();
function ensureWS(){
  if(_wsLock) return;
  if(ws&&(ws.readyState===WebSocket.OPEN||ws.readyState===WebSocket.CONNECTING))return;
  
  _wsLock = true;
  if(ws){
    ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null;
    try{ws.close();}catch(e){}
  }

  console.log('[WS] Connecting to', SIGNAL_URL);
  ws=new WebSocket(SIGNAL_URL);
  
  // Тайм-аут на установку соединения (если за 10с не подключился — пробуем снова)
  const connTimeout = setTimeout(() => {
    if(ws && ws.readyState !== WebSocket.OPEN){
        console.warn('[WS] Connection timeout');
        _wsLock = false;
        ensureWS();
    }
  }, 10000);

  ws.onopen=()=>{
    clearTimeout(connTimeout);
    _wsLock = false;
    wsUp=true;wsRetry=0;
    // ОПТИМИЗАЦИЯ: Используем 'auth' вместо 'register' для единообразия с сервером
    ws.send(JSON.stringify({type:'auth',id:MY_ID}));
    updateSendBtn();
    if(activePid)wsSend({type:'query-presence',target:activePid});
    processOutbox();
    if(pingInterval)clearInterval(pingInterval);
    pingInterval=setInterval(()=>{
        if(ws&&ws.readyState===WebSocket.OPEN) {
            ws.send(JSON.stringify({type:'ping'}));
            // Если понг не приходил слишком долго (более 40с), значит соединение "протухло"
            if(Date.now() - _lastPong > 40000) {
                console.warn('[WS] Zombie connection detected');
                ws.close();
            }
        }
    }, 25000);
  };

  ws.onmessage=async e=>{
    let d;
    try{d=JSON.parse(e.data);}catch{return;}
    if(d.type === 'pong') {
        // Понг пришел — значит связь жива
        _lastPong = Date.now();
        return;
    }
    await onWS(d);
  };

  ws.onclose=(e)=>{
    clearTimeout(connTimeout);
    _wsLock = false;
    wsUp=false;
    updateSendBtn();
    console.log('[WS] Closed:', e.code, e.reason);
    
    if(activePid&&currentScreen==='scr-chat'){
        _stopConnDots();
        _startConnDots('Переподключение');
    }
    if(pingInterval){clearInterval(pingInterval);pingInterval=null;}
    
    // Экспоненциальная задержка с джиттером (чтобы не все сразу ломились на сервер)
    const delay = Math.min(30000, (Math.pow(2, wsRetry) * 1000) + (Math.random() * 1000));
    wsRetry++;
    setTimeout(ensureWS, delay);
  };

  ws.onerror=(err)=>{
    _wsLock = false;
    wsUp=false;
    console.error('[WS] Error:', err);
  };
}

// ОПТИМИЗАЦИЯ 8: window.online/offline события
// При восстановлении интернета — сразу сбрасываем счётчик retry и reconnect
window.addEventListener('online', () => {
  _netQuality.update();
  wsRetry = 0;
  setTimeout(ensureWS, 300);
});
window.addEventListener('offline', () => {
  _netQuality.update();
});
// УЛЬТИМАТИВНАЯ ОПТИМИЗАЦИЯ: Request-Response Deduplication & Binary Payload
const _SENT_LOG_KEY = 'bc_sent_log_v1';
const _SENT_LOG_MAX = 50;
const _sentLog = lsGet(_SENT_LOG_KEY, []);
function _addToSentLog(obj) {
  if (!obj.requestId) return;
  _sentLog.push({ id: obj.requestId, ts: Date.now() });
  if (_sentLog.length > _SENT_LOG_MAX) _sentLog.shift();
  lsSet(_SENT_LOG_KEY, _sentLog);
}

function wsSend(obj){
  // Присваиваем уникальный ID каждому запросу для дедупликации на сервере
  if (!obj.requestId && obj.type !== 'ping') obj.requestId = uid();
  
  // Дренируем буфер перед отправкой нового пакета
  if (ws && ws.readyState === WebSocket.OPEN && window._wsSendBuffer.length > 0) {
    const buf = window._wsSendBuffer.splice(0);
    const now = Date.now();
    for (const item of buf) {
      if (now - item.ts < OUTBOX_TTL) {
        try { ws.send(JSON.stringify(item.obj)); } catch(e) {}
      }
    }
  }
  if (ws && ws.readyState === WebSocket.OPEN) {
    try {
        ws.send(JSON.stringify(obj));
        _addToSentLog(obj);
    } catch(e) {
        if (_WS_BUFFER_TYPES.has(obj.type) && window._wsSendBuffer.length < _WS_BUFFER_MAX) {
            window._wsSendBuffer.push({ obj, ts: Date.now() });
        }
    }
  } else if (_WS_BUFFER_TYPES.has(obj.type) && window._wsSendBuffer.length < _WS_BUFFER_MAX) {
    window._wsSendBuffer.push({ obj, ts: Date.now() });
  }
}

// ── ОПТИМИЗАЦИЯ 1: Генерация миниатюры (thumb) 20x20 для мгновенного превью ──
// Клиент генерирует маленькую картинку и вставляет её в store-file-header.
// Получатель видит её мгновенно — ещё до того как начнётся загрузка чанков.
async function generateThumb(fileInfo){
  try{
    // Для изображений — сжимаем через canvas
    if(fileInfo.type&&fileInfo.type.startsWith('image/')){
      return new Promise(res=>{
        const img=new Image();
        img.onload=()=>{
          try{
            const c=document.createElement('canvas');
            c.width=20;c.height=20;
            const ctx=c.getContext('2d');
            // Вписываем с сохранением пропорций
            const ratio=Math.min(20/img.width,20/img.height);
            const w=img.width*ratio,h=img.height*ratio;
            const x=(20-w)/2,y=(20-h)/2;
            ctx.fillStyle='#1a1a2e';
            ctx.fillRect(0,0,20,20);
            ctx.drawImage(img,x,y,w,h);
            res(c.toDataURL('image/jpeg',0.5));
          }catch(e){res('');}
        };
        img.onerror=()=>res('');
        // Используем data URL если есть, иначе blob
        if(fileInfo.data)img.src=fileInfo.data;
        else if(fileInfo.blob)img.src=URL.createObjectURL(fileInfo.blob);
        else res('');
      });
    }
    // Для видео — захватываем первый кадр
    if(fileInfo.type&&fileInfo.type.startsWith('video/')&&fileInfo.blob){
      return new Promise(res=>{
        const video=document.createElement('video');
        video.muted=true;
        video.playsInline=true;
        const url=URL.createObjectURL(fileInfo.blob);
        video.src=url;
        video.currentTime=0.1;
        const cleanup=()=>{try{URL.revokeObjectURL(url);}catch(e){}};
        video.onseeked=()=>{
          try{
            const c=document.createElement('canvas');
            c.width=20;c.height=20;
            const ctx=c.getContext('2d');
            const ratio=Math.min(20/video.videoWidth,20/video.videoHeight);
            const w=video.videoWidth*ratio,h=video.videoHeight*ratio;
            const x=(20-w)/2,y=(20-h)/2;
            ctx.fillStyle='#1a1a2e';
            ctx.fillRect(0,0,20,20);
            ctx.drawImage(video,x,y,w,h);
            cleanup();
            res(c.toDataURL('image/jpeg',0.5));
          }catch(e){cleanup();res('');}
        };
        video.onerror=()=>{cleanup();res('');};
        setTimeout(()=>{cleanup();res('');},3000);
        video.load();
      });
    }
  }catch(e){}
  return '';
}

// ── UPLOAD PROGRESS UI ──
const uploadUIs=new Map();
function registerUploadUI(fileId,spinnerEl,fillEl,pctEl){uploadUIs.set(fileId,{spinnerEl,fillEl,pctEl});}
function updateUploadProgress(fileId,pct){const ui=uploadUIs.get(fileId);if(!ui)return;ui.fillEl.style.strokeDashoffset=Math.max(0,132-132*pct/100);if(ui.pctEl)ui.pctEl.textContent=pct+'%';}
async function finalizeUploadUI(fileId,pid){
  const ui=uploadUIs.get(fileId);
  if(ui){
    ui.fillEl.style.strokeDashoffset=0;
    const iconEl=ui.spinnerEl.querySelector('.upload-spinner-icon,.file-upload-spinner-icon');
    if(iconEl){iconEl.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M20 6 9 17l-5-5\" /> </svg>';iconEl.style.color='var(--g)';}
    ui.spinnerEl.classList.add('spinner-done');
    await new Promise(r=>setTimeout(r,350));
    uploadUIs.delete(fileId);
  }
  if(activePid===pid){
    const msgs=await loadMsgs(pid);
    const m=msgs.find(x=>x.id===fileId);
    if(m){const existingRow=document.querySelector(`.msg-row[data-msgid="${fileId}"]`);if(existingRow)(existingRow.closest('.msg-row-outer')||existingRow).replaceWith(buildRow(m));}
  }
}

// ── ФАЙЛОВЫЙ ПРОТОКОЛ ──
const fileTransfers=new Map();
const storeAckWaiters=new Map();
// Map fileId → peerId: нужен для file-delivered чтобы обновить ✔✔
// без зависимости от activePid
const _fileSentToPeer=new Map();

// Отправка сигнала активности (ephemeral, не сохраняется)
async function _sendActivitySignal(pid,activityType){
  const key=keyCache[pid];if(!key||!wsUp)return;
  try{const enc=await encData(ENC.encode(JSON.stringify({type:'activity',activityType,ts:Date.now()})),key);wsSend({type:'send-msg',target:pid,msgId:uid(),payload:payloadToB64(enc),ephemeral:true});}catch(e){}
}

// ── Heartbeat "выбирает стикер..." — отправляем каждые 3с пока панель открыта ──
let _stickerActivityInterval=null;

function _startStickerActivity(){
  // Немедленно отправляем первый сигнал
  if(activePid&&wsUp)_sendActivitySignal(activePid,'choosing_sticker');
  // Потом каждые 3 секунды — чтобы таймер автосброса (5с) не успел погасить статус
  if(_stickerActivityInterval)clearInterval(_stickerActivityInterval);
  _stickerActivityInterval=setInterval(()=>{
    const panel=$('stickerPanel');
    if(!panel||!panel.classList.contains('open')){
      _stopStickerActivity();
      return;
    }
    if(activePid&&wsUp)_sendActivitySignal(activePid,'choosing_sticker');
  },3000);
}

function _stopStickerActivity(){
  if(_stickerActivityInterval){
    clearInterval(_stickerActivityInterval);
    _stickerActivityInterval=null;
  }
  // Отправляем null-сигнал чтобы собеседник немедленно убрал статус
  if(activePid&&wsUp)_sendActivitySignal(activePid,null);
}
async function _sendActivityStop(pid){await _sendActivitySignal(pid,null);}

// ИСПРАВЛЕНИЕ 2: читаем blob → data: URL ДО сохранения в IndexedDB
async function sendFileToServer(fileInfo,caption){
  const key=await ensureKey(activePid);
  const fileId=uid();
  const ts=Date.now();

  // Сохраняем содержимое файла в blobs-хранилище IDB (без data:URL в основных записях)
  // fileBuffer читается ниже для чанков — сохраняем его в blobs заранее

  const m={
    id:fileId,text:caption||'',type:'sent',
    time:new Date(ts).toISOString(),reactions:{},edited:false,
    replyTo:replyTo||null,delivered:false,forwarded:false,
    uploading:true,
    file:{name:fileInfo.name,type:fileInfo.type,size:fileInfo.size,blobId:fileId}
  };
  await upsertMsg(activePid,m);
  await updateChat(activePid,{lastMsg:`${fileInfo.name}`,lastMsgTime:ts});
  appendOrReloadMsg(activePid,m);

  // Сигнал "отправляет файл" + heartbeat каждые 3с
  _sendActivitySignal(activePid,'sending_file');
  const _fileActivityHb=setInterval(()=>{if(activePid)_sendActivitySignal(activePid,'sending_file');},3000);

  const fileBuffer=await readFileAsArrayBuffer(fileInfo.blob);
  const totalChunks=Math.ceil(fileBuffer.byteLength/CHUNK_SIZE);
  // Сохраняем blob отправителя в IDB 'blobs' чтобы можно было воспроизвести без data:URL
  await saveBlob(fileId, fileBuffer);

  // ОПТИМИЗАЦИЯ 1: Генерируем thumb до отправки заголовка
  // Получатель увидит превью мгновенно, ещё до первого чанка
  const thumb = await generateThumb(fileInfo).catch(()=>'');

  wsSend({type:'store-file-header',fileId,recipientId:activePid,name:fileInfo.name,size:fileInfo.size,mimeType:fileInfo.type,totalChunks,caption:caption||'',thumb:thumb||'',ts});
  // Запоминаем кому отправлен файл — нужно для file-delivered → ✔✔
  _fileSentToPeer.set(fileId, activePid);
  // Сохраняем в localStorage — пережить перезагрузку страницы
  try{const _sftp=lsGet('_fileSentToPeer',{});_sftp[fileId]=activePid;lsSet('_fileSentToPeer',_sftp);}catch(e){}

  await new Promise(resolve=>{const timer=setTimeout(resolve,30000);storeAckWaiters.set(fileId,{resolve:()=>{clearTimeout(timer);resolve();},timer});});
  storeAckWaiters.delete(fileId);

  // Шифруем и отправляем чанки батчами — не блокируем UI
  // Каждый батч: зашифровать STORE_BATCH чанков → отправить → yield
  for(let b=0;b<totalChunks;b+=STORE_BATCH){
    const batch=[];
    const end=Math.min(b+STORE_BATCH,totalChunks);
    for(let i=b;i<end;i++){
      const start=i*CHUNK_SIZE;
      const slice=fileBuffer.slice(start,Math.min(start+CHUNK_SIZE,fileBuffer.byteLength));
      const encBuf=await encData(slice,key);
      batch.push({index:i,data:arrayBufferToBase64(encBuf)});
    }
    wsSend({type:'store-chunks',fileId,chunks:batch});
    const pct=Math.round((end/totalChunks)*100);
    updateUploadProgress(fileId,pct);
    // Yield UI thread между батчами чтобы интерфейс не замерзал
    if(end<totalChunks) await new Promise(r=>setTimeout(r,0));
  }

  // Снимаем флаг uploading
  await withChatLock(activePid,async()=>{
    const msgs=await loadMsgs(activePid);
    const idx=msgs.findIndex(x=>x.id===fileId);
    if(idx!==-1){msgs[idx].uploading=false;await saveMsgs(activePid,msgs);}
  });

  // НЕМЕДЛЕННО финализируем UI у отправителя — не ждём file-upload-complete от сервера
  await finalizeUploadUI(fileId,activePid);
  clearInterval(_fileActivityHb);
  _sendActivityStop(activePid);
  setTimeout(()=>_sendActivityStop(activePid),30000);
  return fileId;
}

// ИСПРАВЛЕНИЕ: handleFileAvailable — строгая проверка дублей + правильный unread
// ОПТИМИЗАЦИЯ 1+3: thumb показывается мгновенно; chunksReady — сколько чанков уже есть
async function handleFileAvailable(msg){
  const{fileId,senderId,name,size,mimeType,totalChunks,caption,ts,thumb,chunksReady}=msg;
  if(senderId===MY_ID)return;
  const ct=loadContacts().find(c=>c.id===senderId);
  await getOrCreateChat(senderId,ct?.name,ct?.avatar);
  const existing=await loadMsgs(senderId);
  if(existing.find(m=>m.id===fileId)){
    // Дубль — только обновляем DOM если нужно, счётчик НЕ трогаем
    if(currentScreen==='scr-chat'&&activePid===senderId){
      const existingRow=document.querySelector(`.msg-row[data-msgid="${fileId}"]`);
      if(!existingRow){
        const placeholderMsg=existing.find(m=>m.id===fileId);
        if(placeholderMsg)appendOrReloadMsg(senderId,placeholderMsg);
      }
    }
    return;
  }

  // ── Детект видео-кружка ──
  // Имя файла начинается с '__vnote__' — это видео-кружок, отправленный через чанковый протокол
  if(name&&name.startsWith(VN_FILE_PREFIX)){
    let vnMeta={durSec:0,videoMime:mimeType||'video/webm',replyTo:null};
    try{ if(caption) vnMeta={...vnMeta,...JSON.parse(caption)}; }catch(e){}
    // НЕ скачиваем автоматически — показываем кнопку скачивания (как у файлов)
    const vnMsg={
      id:fileId,text:'',type:'recv',
      time:new Date(ts||Date.now()).toISOString(),
      reactions:{},edited:false,replyTo:vnMeta.replyTo||null,
      delivered:true,forwarded:false,
      videoNote:true,voiceDuration:vnMeta.durSec||0,voiceListened:false,
      videoMime:vnMeta.videoMime||mimeType||'video/webm',
      // videoBlobId будет установлен после скачивания
      _vnPending:true,_vnFileId:fileId,_vnSenderId:senderId,
      _vnSize:size,_vnTotalChunks:totalChunks,
    };
    await upsertMsg(senderId,vnMsg);
    await updateChat(senderId,{lastMsg:'Видео-кружок',lastMsgTime:new Date(vnMsg.time).getTime()});
    if(currentScreen==='scr-chat'&&activePid===senderId){
      const existingRow=document.querySelector(`.msg-row[data-msgid="${fileId}"]`);
      if(!existingRow)appendOrReloadMsg(senderId,vnMsg);
    }else{
      const chat=(await loadChats()).find(c=>c.peerId===senderId);
      await updateChat(senderId,{unread:(chat?.unread||0)+1});
      toast(`${chat?.peerName||senderId}: Видео-кружок`);
    }
    return;
  }

  // ОПТИМИЗАЦИЯ 1: сохраняем thumb в сообщении — покажется мгновенно
  // ОПТИМИЗАЦИЯ 3: chunksReady — сколько чанков уже готово (может быть не все)
  const placeholderMsg={id:fileId,text:caption||'',type:'recv',time:new Date(ts||Date.now()).toISOString(),reactions:{},edited:false,replyTo:null,delivered:true,forwarded:false,fileAvailable:{name,size,mimeType,totalChunks,senderId,thumb:thumb||'',chunksReady:chunksReady||0}};
  await upsertMsg(senderId,placeholderMsg);
  await updateChat(senderId,{lastMsg:`${name}`,lastMsgTime:new Date(placeholderMsg.time).getTime()});
  if(currentScreen==='scr-chat'&&activePid===senderId){
    const existingRow=document.querySelector(`.msg-row[data-msgid="${fileId}"]`);
    if(!existingRow)appendOrReloadMsg(senderId,placeholderMsg);
  }else{
    const chat=(await loadChats()).find(c=>c.peerId===senderId);
    await updateChat(senderId,{unread:(chat?.unread||0)+1});
    toast(`${chat?.peerName||senderId}: ${name}`);
  }
}

// Скачиваем видео-кружок в фоне и обновляем сообщение
async function _downloadVideoNote(fileId, senderId, videoMime){
  try{
    // Используем существующий механизм скачивания файлов
    const key=await getKey(senderId);
    if(!key){console.warn('[vnote] no key for',senderId);return;}
    fileTransfers.set(fileId,{state:'downloading',chunks:[],total:0,received:0,key,senderId,retries:0,maxRetries:6,_isVideoNote:true,_vnMime:videoMime});
    wsSend({type:'fetch-file',fileId});
    const ft=fileTransfers.get(fileId);
    ft._timeoutTimer=setTimeout(()=>_retryDownload(fileId),30000);
  }catch(e){console.error('[vnote] download error',e);}
}

async function downloadFileFromServer(fileId,senderId){
  const key=await getKey(senderId);if(!key){toast('Нет ключа шифрования — введите пароль','err');return;}
  
  // ФИКС: Не перезаписываем объект, если он уже есть (чтобы не терять прогресс)
  let ft = fileTransfers.get(fileId);
  if (!ft) {
    ft = {state:'downloading',chunks:[],total:0,received:0,key,senderId,retries:0,maxRetries:5};
    fileTransfers.set(fileId, ft);
  } else {
    ft.state = 'downloading';
    ft.retries = 0;
  }
  
  // ФИКС: Если мы уже что-то скачали, запрашиваем с нужного индекса
  const fromIndex = ft._lastReceivedIndex !== undefined ? ft._lastReceivedIndex + 1 : (ft.received || 0);
  wsSend({type:'fetch-file',fileId, fromIndex});
  
  if(ft._timeoutTimer) clearTimeout(ft._timeoutTimer);
  ft._timeoutTimer=setTimeout(()=>_retryDownload(fileId),30000);
}

async function _retryDownload(fileId){
  const ft=fileTransfers.get(fileId);
  if(!ft||ft.state==='done')return;

  // ОПТИМИЗАЦИЯ: Если очередь дешифровки ещё работает, не паникуем!
  // Это значит, что чанки прилетели по сети, но CPU ещё занят их обработкой.
  const q = _decryptQueue.get(fileId);
  if (q && q.queue.length > 0) {
    console.log(`[File] Decrypt queue is busy (${q.queue.length} left), delaying retry for ${fileId}`);
    if(ft._timeoutTimer) clearTimeout(ft._timeoutTimer);
    ft._timeoutTimer = setTimeout(() => _retryDownload(fileId), 10000); // Ждём ещё 10с
    return;
  }

  // Убираем ограничение на количество попыток — теперь бесконечно!
  ft.retries++;
  ft.state='downloading';
  
  const fromIndex = ft._lastReceivedIndex !== undefined ? ft._lastReceivedIndex + 1 : (ft.received || 0);
  
  console.warn(`[File] Retry ${ft.retries} for ${fileId}, resuming from chunk ${fromIndex}/${ft.total}`);
  if(ft._chunksReady!==undefined&&ft._chunksReady<ft.total&&fromIndex>=ft._chunksReady){
    // Мы скачали всё что было на сервере, просто ждём новых чанков
    console.log(`[File] Waiting for new chunks on server (${ft._chunksReady}/${ft.total})`);
    toast(`Продолжаю скачивать…`,'warn');
  } else {
    toast(`Продолжаю загрузку…`,'warn');
  }
  if(!wsUp){
    const _wait=()=>{if(wsUp){wsSend({type:'fetch-file',fileId,fromIndex});}else setTimeout(_wait,1000);};
    setTimeout(_wait,1000);
  } else {
    wsSend({type:'fetch-file',fileId,fromIndex});
  }
  if(ft._timeoutTimer) clearTimeout(ft._timeoutTimer);
  ft._timeoutTimer=setTimeout(()=>_retryDownload(fileId), 30000);
}

async function handleFileDataHeader(msg){
  const{fileId,senderId,name,size,mimeType,totalChunks,caption,ts,fromIndex,chunksReady}=msg;
  const ft=fileTransfers.get(fileId);if(!ft)return;
  
  ft.total=totalChunks;
  // ФИКС: Инициализируем массив чанков только если он пустой
  if(!ft.chunks || ft.chunks.length === 0){
    ft.chunks = new Array(totalChunks).fill(null);
  } else if (ft.chunks.length !== totalChunks) {
    // Если размер изменился (странно, но бывает), расширяем
    const newArr = new Array(totalChunks).fill(null);
    for(let i=0; i<Math.min(ft.chunks.length, totalChunks); i++) newArr[i] = ft.chunks[i];
    ft.chunks = newArr;
  }

  if(ft.received === undefined) ft.received = 0;
  ft.name=name;ft.size=size;ft.mimeType=mimeType;ft.caption=caption;ft.ts=ts;ft.senderId=senderId;
  ft._resumeFromIndex=fromIndex||0;
  ft._chunksReady=chunksReady||totalChunks;
  
  console.log(`[File] Header for ${fileId}: ${totalChunks} chunks, ready on server: ${chunksReady}, resuming from ${fromIndex}`);
  
  if(ft._timeoutTimer){clearTimeout(ft._timeoutTimer);ft._timeoutTimer=null;}
  ft._timeoutTimer=setTimeout(()=>_retryDownload(fileId),30000);
}

// Очередь декрипта чанков — обрабатываем по одному чтобы не блокировать UI
const _decryptQueue=new Map(); // fileId -> {queue:[], running:bool}
async function _processDecryptQueue(fileId){
  const q=_decryptQueue.get(fileId);
  if(!q||q.running)return;
  q.running=true;
  
  // ОПТИМИЗАЦИЯ: Обрабатываем чанки пачками по 5 штук параллельно
  // Это значительно ускоряет процесс на многоядерных CPU и не дает UI зависнуть
  while(q.queue.length>0){
    const batch = q.queue.splice(0, 5);
    await Promise.all(batch.map(async (msg) => {
      const ft=fileTransfers.get(msg.fileId);
      if(!ft) return;
      try{
        const encBuf=base64ToArrayBuffer(msg.data);
        const decBuf=await decData(encBuf,ft.key);
        ft.chunks[msg.index]=decBuf;
        ft.received=(ft.received||0)+1;
        
        // Сбрасываем тайм-аут ожидания
        if(ft._timeoutTimer){clearTimeout(ft._timeoutTimer);ft._timeoutTimer=null;}
        if(ft.received < ft.total){
          ft._timeoutTimer=setTimeout(()=>_retryDownload(msg.fileId),30000);
        }
      }catch(e){console.warn(`chunk decrypt error index=${msg.index}`,e);}
    }));

    const ft=fileTransfers.get(fileId);
    if(ft) {
      const pct=Math.round((ft.received/ft.total)*100);
      updateSpinnerProgress(fileId,pct);
      
      // ФИКС: Проверяем готовность к сборке.
      // Если это последний чанк, запускаем сборку немедленно.
      const isComplete = ft.received >= ft.total && ft.chunks.every(c => c !== null);
      if(isComplete && ft.state !== 'done' && ft.state !== 'assembling'){
        console.log(`[File] All chunks received for ${fileId}, assembling...`);
        if(ft._timeoutTimer){clearTimeout(ft._timeoutTimer);ft._timeoutTimer=null;}
        await assembleFile(fileId);
      } else if (ft.received < ft.total) {
        // Если мы скачали всё что было на сервере, но файл не закончен,
        // запускаем retry через 30 секунд.
        if (ft.received >= ft._chunksReady) {
           if(ft._timeoutTimer) clearTimeout(ft._timeoutTimer);
           ft._timeoutTimer = setTimeout(() => _retryDownload(fileId), 30000);
        }
      }
    }
    await new Promise(r=>setTimeout(r,0));
  }
  q.running=false;
  _decryptQueue.delete(fileId);
}

async function handleFileDataChunk(msg){
  const{fileId,index,total,data}=msg;
  const ft=fileTransfers.get(fileId);if(!ft)return;
  if(!ft.chunks)ft.chunks=new Array(total).fill(null);
  if(ft.chunks[index]!==null)return;
  
  // ФИКС: Запоминаем индекс последнего прилетевшего по сети чанка.
  // Если случится Retry, мы продолжим с index + 1, даже если этот чанк ещё не дешифрован.
  ft._lastReceivedIndex = Math.max(ft._lastReceivedIndex || 0, index);

  // Ставим в очередь — не await сразу чтобы WS мог принять следующий chunk
  if(!_decryptQueue.has(fileId))_decryptQueue.set(fileId,{queue:[],running:false});
  _decryptQueue.get(fileId).queue.push({fileId,index,total,data});
  _processDecryptQueue(fileId); // fire-and-forget
}

async function assembleFile(fileId){
  const ft=fileTransfers.get(fileId);if(!ft||ft.state==='done')return;ft.state='assembling';
  try{
    // ОПТИМИЗАЦИЯ 4: собираем чанки через Blob — не нужно копировать все данные в Uint8Array
    // Браузер собирает Blob эффективно в памяти, без лишних копий
    const blobParts = ft.chunks.map(buf => new Blob([buf]));
    const combinedBlob = new Blob(blobParts, {type: ft.mimeType || 'application/octet-stream'});
    // Получаем ArrayBuffer только для сохранения в IDB
    const combined = { buffer: await combinedBlob.arrayBuffer() };

    // ── Видео-кружок: особая обработка ──────────────────────────────────────
    if(ft._isVideoNote){
      const vnMime=ft._vnMime||'video/webm';
      const vnBlobId=fileId+'_vnote';
      try{
        await saveBlob(vnBlobId,combined.buffer);
      }catch(e){
        console.error('[File] Failed to save video note blob:',e);
        toast('Ошибка сохранения видео-кружка','err');
        fileTransfers.delete(fileId);
        return;
      }
      // ОПТИМИЗАЦИЯ 4: используем уже готовый combinedBlob для ObjectURL
      const vnBlob=new Blob([combinedBlob],{type:vnMime});
      const vnUrl=URL.createObjectURL(vnBlob);
      const finalMsg={
        id:fileId,text:'',type:'recv',
        time:new Date(ft.ts||Date.now()).toISOString(),
        reactions:{},edited:false,replyTo:null,delivered:true,forwarded:false,
        videoNote:true,
        voiceDuration:0, // durSec будет в ft.caption (JSON)
        voiceListened:false,
        videoBlobId:vnBlobId,
        videoMime:vnMime,
        _vnPending:false,
        _readReceiptSent:true,
      };
      // Восстанавливаем durSec из caption
      try{const meta=JSON.parse(ft.caption||'{}');if(meta.durSec)finalMsg.voiceDuration=meta.durSec;}catch(e){}
      await upsertMsg(ft.senderId,finalMsg);
      await updateChat(ft.senderId,{lastMsg:'Видео-кружок',lastMsgTime:new Date(finalMsg.time).getTime()});
      if(currentScreen==='scr-chat'&&activePid===ft.senderId){
        const existingRow=document.querySelector(`.msg-row[data-msgid="${fileId}"]`);
        const renderMsg={...finalMsg,videoData:vnUrl};
        if(existingRow)(existingRow.closest('.msg-row-outer')||existingRow).replaceWith(buildRow(renderMsg));
        else appendOrReloadMsg(ft.senderId,renderMsg);
      }
      // ИСПРАВЛЕНИЕ: Отправляем ack-file ТОЛЬКО ПОСЛЕ сохранения в IDB
      wsSend({type:'ack-file',fileId});
      fileTransfers.delete(fileId);
      // Уведомляем отправителя → ✔✔
      const _vnKey=await getKey(ft.senderId).catch(()=>null);
      if(_vnKey&&wsUp&&ft.senderId!==MY_ID){
        try{
          const _vnEnc=await encData(ENC.encode(JSON.stringify({type:'file-delivered',fileId,ts:Date.now()})),_vnKey);
          wsSend({type:'send-msg',target:ft.senderId,msgId:uid(),payload:payloadToB64(_vnEnc)});
        }catch(e){}
      }
      return;
    }

    // ОПТИМИЗАЦИЯ 4: используем уже готовый combinedBlob — не создаём лишних копий
    const _combinedBlob=combinedBlob;
    const _blobUrl=URL.createObjectURL(_combinedBlob);
    try{
      await saveBlob(fileId,combined.buffer);
    }catch(e){
      console.error('[File] Failed to save blob:',e);
      toast('Ошибка сохранения файла','err');
      fileTransfers.delete(fileId);
      return;
    }
    const finalMsg={id:fileId,text:ft.caption||'',type:'recv',time:new Date(ft.ts||Date.now()).toISOString(),reactions:{},edited:false,replyTo:null,delivered:true,forwarded:false,fileAvailable:null,_readReceiptSent:true,file:{name:ft.name,type:ft.mimeType,size:ft.size,blobId:fileId}};
    await upsertMsg(ft.senderId,finalMsg);
    await updateChat(ft.senderId,{lastMsg:`${ft.name}`,lastMsgTime:new Date(finalMsg.time).getTime()});
    const renderMsg={...finalMsg,file:{...finalMsg.file,_tempUrl:_blobUrl}};
    if(currentScreen==='scr-chat'&&activePid===ft.senderId){
      const existingRow=document.querySelector(`.msg-row[data-msgid="${fileId}"]`);
      if(existingRow)(existingRow.closest('.msg-row-outer')||existingRow).replaceWith(buildRow(renderMsg));
      else appendOrReloadMsg(ft.senderId,renderMsg);
    }
    // ИСПРАВЛЕНИЕ: Отправляем ack-file ТОЛЬКО ПОСЛЕ сохранения в IDB
    wsSend({type:'ack-file',fileId});
    fileTransfers.delete(fileId);
    toast(`${ft.name} загружен`);
    const _fdKey=await getKey(ft.senderId).catch(()=>null);
    if(_fdKey&&wsUp&&ft.senderId!==MY_ID){
      try{
        const _fdEnc=await encData(ENC.encode(JSON.stringify({type:'file-delivered',fileId,ts:Date.now()})),_fdKey);
        wsSend({type:'send-msg',target:ft.senderId,msgId:uid(),payload:payloadToB64(_fdEnc)});
      }catch(e){}
    }
  }catch(e){console.error('assembleFile error',e);toast('Ошибка сборки файла','err');const spinner=document.querySelector(`.tg-spinner[data-fileid="${fileId}"]`);if(spinner)resetSpinnerToDownload(spinner,fileId,ft.senderId);}
  ft.state='done';
}

function updateSpinnerProgress(fileId, pct) {
  // ФИКС: Запоминаем максимальный достигнутый процент, чтобы не было "откатов" в 0%
  if (!window._fileMaxPct) window._fileMaxPct = new Map();
  const currentMax = window._fileMaxPct.get(fileId) || 0;
  if (pct < currentMax) {
    // Если новый процент меньше текущего максимума (например, при начале скачивания),
    // мы игнорируем его, чтобы не пугать пользователя прыжком в 0.
    return;
  }
  window._fileMaxPct.set(fileId, pct);

  const spinner = document.querySelector(`.tg-spinner[data-fileid="${fileId}"]`);
  if (spinner) {
    const fill = spinner.querySelector('.tg-spinner-fill');
    if (fill) fill.style.strokeDashoffset = Math.max(0, 122 - 122 * pct / 100);
    const icon = spinner.querySelector('.tg-spinner-icon');
    if (icon && pct >= 100) { icon.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M20 6 9 17l-5-5\" /> </svg>'; icon.style.color = 'var(--g)'; }
  }
  // Видео-кружок: спиннер по центру кружка
  const vnSpinner = document.querySelector(`.vn-dl-spinner[data-fileid="${fileId}"]`);
  if (vnSpinner) {
    const fill = vnSpinner.querySelector('.vn-dl-spinner-fill');
    if (fill) fill.style.strokeDashoffset = Math.max(0, 122 - 122 * pct / 100);
    const icon = vnSpinner.querySelector('.vn-dl-spinner-icon');
    if (icon && pct >= 100) { icon.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M20 6 9 17l-5-5\" /> </svg>'; icon.style.color = 'var(--g)'; }
  }
}
function resetSpinnerToDownload(spinner,fileId,fromId){
  const iconWrap=document.createElement('div');
  // Поддерживаем и старый .file-card и новый .tg-file-wrap
  const isNewStyle = spinner.closest('.tg-file-wrap,.tg-audio-pending-wrap');
  if(isNewStyle){
    iconWrap.className='tg-file-icon tg-file-icon-dl';
    iconWrap.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 15V3\" /> <path d=\"M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4\" /> <path d=\"m7 10 5 5 5-5\" /> </svg>';
  } else {
    iconWrap.className='file-icon-wrap file-icon-download';
    const arrow=document.createElement('div');
    arrow.className='file-dl-arrow';
    arrow.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 15V3\" /> <path d=\"M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4\" /> <path d=\"m7 10 5 5 5-5\" /> </svg>';
    iconWrap.appendChild(arrow);
  }
  iconWrap.onclick=e=>{e.stopPropagation();startDownloadUI(iconWrap,fileId,fromId);};
  spinner.replaceWith(iconWrap);
}
function startDownloadUI(iconWrap,fileId,fromId){
  const spinner=document.createElement('div');
  spinner.className='tg-spinner';
  spinner.dataset.fileid=fileId;
  spinner.innerHTML=`<svg viewBox="0 0 42 42"><circle class="tg-spinner-track" cx="21" cy="21" r="19"/><circle class="tg-spinner-fill" cx="21" cy="21" r="19" stroke-dasharray="122" stroke-dashoffset="122"/></svg><div class="tg-spinner-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M12 15V3" /> <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /> <path d="m7 10 5 5 5-5" /> </svg></div>`;
  const card=iconWrap.closest('.tg-file-wrap,.tg-audio-pending-wrap,.file-card');
  iconWrap.replaceWith(spinner);
  if(card){const sizeEl=card.querySelector('.tg-file-size,.file-size');if(sizeEl)sizeEl.textContent='Загрузка…';}
  downloadFileFromServer(fileId,fromId);
}

// handleFileRef — запасной путь, если file-available не пришёл (оффлайн-кейс).
// Строгая проверка: если сообщение уже есть — пропускаем.
async function handleFileRef(from,env){
  const{id:fileId,text,ts,replyTo:rt,file:finfo}=env;
  const ct=loadContacts().find(c=>c.id===from);
  await getOrCreateChat(from,ct?.name,ct?.avatar);
  const existing=await loadMsgs(from);
  if(existing.find(m=>m.id===fileId)){
    // Дубль — только обновляем DOM если нужно, счётчик НЕ трогаем
    if(currentScreen==='scr-chat'&&activePid===from){
      const existingRow=document.querySelector(`.msg-row[data-msgid="${fileId}"]`);
      if(!existingRow){
        const placeholderMsg=existing.find(m=>m.id===fileId);
        if(placeholderMsg)appendOrReloadMsg(from,placeholderMsg);
      }
    }
    return;
  }
  const placeholderMsg={id:fileId,text:text||'',type:'recv',time:new Date(ts||Date.now()).toISOString(),reactions:{},edited:false,replyTo:rt||null,delivered:true,forwarded:false,fileAvailable:{name:finfo.name,size:finfo.size,mimeType:finfo.mimeType,totalChunks:finfo.totalChunks,senderId:from}};
  await upsertMsg(from,placeholderMsg);
  await updateChat(from,{lastMsg:`${finfo.name}`,lastMsgTime:new Date(placeholderMsg.time).getTime()});
  if(currentScreen==='scr-chat'&&activePid===from){
    const existingRow=document.querySelector(`.msg-row[data-msgid="${fileId}"]`);
    if(!existingRow)appendOrReloadMsg(from,placeholderMsg);
  }else{
    const chat=(await loadChats()).find(c=>c.peerId===from);
    await updateChat(from,{unread:(chat?.unread||0)+1,marked:false});
    toast(`${chat?.peerName||from}: ${finfo.name}`);
  }
}

// ── WS ──
async function registerPushNotifications(){if(!window.OneSignal)return;try{const granted=await OneSignal.Notifications.requestPermission();if(granted){const pid=await OneSignal.User.getOnesignalId();if(pid)wsSend({type:'register-push',playerId:pid});}}catch(e){}}

async function markDelivered(msgId,peerId,isExplicitFileDelivered){
  if(!peerId) return;
  await withChatLock(peerId, async()=>{
    const msgs = await loadMsgs(peerId);
    const m = msgs.find(m=>m.id===msgId);
    if(m && !m.delivered){
      // ИСПРАВЛЕНИЕ: Файлы и видео-кружки помечаются доставленными (✔✔)
      // ТОЛЬКО через явный сигнал 'file-delivered' от получателя.
      // Игнорируем для них системный 'msg-delivered' от сервера (который
      // срабатывает сразу при попадании сообщения в очередь сервера).
      const isAttachment = m.file || m.videoNote || m.media;
      if(isAttachment && !isExplicitFileDelivered) return;

      m.delivered = true;
      await saveMsgs(peerId, msgs);
    }
  });
  // ✔✔ показываем ТОЛЬКО если получатель сейчас смотрит на этот чат
  // (то есть отправитель открыл чат и видит сообщение на экране)
  if(activePid === peerId && currentScreen === 'scr-chat'){
    const row = document.querySelector(`.msg-row[data-msgid="${msgId}"]`);
    if(row){
      const tk = row.querySelector('.msg-ticks');
      if(tk){ tk.innerHTML='<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M18 6 7 17l-5-5" /> <path d="m22 10-7.5 7.5L13 16" /> </svg>'; tk.className='msg-ticks double'; }
    }
  }
  // Иначе — ✔✔ появится когда отправитель откроет чат (через syncDeliveredStatuses)
}
async function markVoiceListened(voiceMsgId,senderPid){if(!senderPid)return;await withChatLock(senderPid,async()=>{if(activePid===senderPid){for(let i=0;i<30;i++){const dot=document.querySelector(`.msg-row[data-msgid="${voiceMsgId}"] .voice-unread-dot`);if(dot){dot.classList.add('hidden');break;}await new Promise(r=>setTimeout(r,100));}}const msgs=await loadMsgs(senderPid);const m=msgs.find(m=>m.id===voiceMsgId);if(m&&m.voice&&!m.voiceListened){m.voiceListened=true;await saveMsgs(senderPid,msgs);}});}

// ── Получатель посмотрел видео-кружок — скрываем dot у отправителя ──
async function markVnWatched(vnMsgId,fromPid){
  if(!fromPid)return;
  // Скрываем ОБА индикатора в DOM немедленно:
  // 1) .vn-unread-dot — точка в мета-строке под кружком
  // 2) .vn-circle-unread-badge — бейдж прямо на кружке (снизу по центру)
  if(activePid===fromPid){
    for(let i=0;i<30;i++){
      const row=document.querySelector(`.msg-row[data-msgid="${vnMsgId}"]`);
      if(row){
        const dot=row.querySelector('.vn-unread-dot');
        const badge=row.querySelector('.vn-circle-unread-badge');
        if(dot)dot.classList.add('hidden');
        if(badge)badge.classList.add('hidden');
        break;
      }
      await new Promise(r=>setTimeout(r,100));
    }
  }
  // Сохраняем в IDB — помечаем как просмотренное
  await withChatLock(fromPid,async()=>{
    const msgs=await loadMsgs(fromPid);
    const m=msgs.find(m=>m.id===vnMsgId);
    if(m&&m.videoNote&&!m.voiceListened){m.voiceListened=true;await saveMsgs(fromPid,msgs);}
  }).catch(()=>{});
}

// ── Надёжная отправка vn-watched (работает офлайн) ──────────────────────────
// Если WS недоступен — сигнал сохраняется в localStorage и
// автоматически отправляется при следующем подключении.
const VN_WATCHED_QUEUE_KEY = 'bc_pending_vn_watched';
const VOICE_LISTENED_QUEUE_KEY = 'bc_pending_voice_listened';

function _enqueueVnWatched(targetPid, vnMsgId){
  const q = lsGet(VN_WATCHED_QUEUE_KEY, []);
  // Не дублируем одинаковые записи
  if(!q.find(x=>x.targetPid===targetPid&&x.vnMsgId===vnMsgId)){
    q.push({targetPid, vnMsgId, ts:Date.now()});
    try{lsSet(VN_WATCHED_QUEUE_KEY, q);}catch(e){}
  }
}

function _enqueueVoiceListened(targetPid, voiceMsgId){
  const q = lsGet(VOICE_LISTENED_QUEUE_KEY, []);
  if(!q.find(x=>x.targetPid===targetPid&&x.voiceMsgId===voiceMsgId)){
    q.push({targetPid, voiceMsgId, ts:Date.now()});
    try{lsSet(VOICE_LISTENED_QUEUE_KEY, q);}catch(e){}
  }
}

// ИСПРАВЛЕНИЕ: Возобновляем все активные загрузки файлов при reconnect
function _resumeAllFileTransfers(){
  if(!wsUp) return;
  const activeTransfers = Array.from(fileTransfers.entries());
  if(activeTransfers.length === 0) return;
  console.log(`[File] Resuming ${activeTransfers.length} file transfers after reconnect`);
  for(const [fileId, ft] of activeTransfers){
    if(ft.state === 'downloading'){
      const fromIndex = ft.received || 0;
      console.log(`[File] Resuming ${fileId}: from chunk ${fromIndex}/${ft.total}`);
      wsSend({type:'fetch-file', fileId, fromIndex});
      // Сбрасываем таймер, чтобы не было двойного retry
      if(ft._timeoutTimer){
        clearTimeout(ft._timeoutTimer);
      }
      ft._timeoutTimer = setTimeout(()=>_retryDownload(fileId), 30000);
    }
  }
}

// Дренируем очереди — вызывается при каждом 'registered' (ws.onopen)
function _drainWatchedQueues(){
  // vn-watched
  const vnQ = lsGet(VN_WATCHED_QUEUE_KEY, []);
  if(vnQ.length){
    const remaining = [];
    for(const item of vnQ){
      if(wsUp && item.targetPid && item.vnMsgId){
        wsSend({type:'vn-watched', target:item.targetPid, vnMsgId:item.vnMsgId});
      } else {
        remaining.push(item);
      }
    }
    try{lsSet(VN_WATCHED_QUEUE_KEY, remaining);}catch(e){}
  }
  // voice-listened
  const vlQ = lsGet(VOICE_LISTENED_QUEUE_KEY, []);
  if(vlQ.length){
    const remaining = [];
    for(const item of vlQ){
      if(wsUp && item.targetPid && item.voiceMsgId){
        wsSend({type:'voice-listened', target:item.targetPid, voiceMsgId:item.voiceMsgId});
      } else {
        remaining.push(item);
      }
    }
    try{lsSet(VOICE_LISTENED_QUEUE_KEY, remaining);}catch(e){}
  }
}

// Главная функция: отправить vn-watched (с offline-гарантией)
function sendVnWatched(targetPid, vnMsgId){
  if(!targetPid || targetPid === MY_ID) return;
  // Всегда ставим в очередь (защита от потери при закрытии вкладки сразу после отправки)
  _enqueueVnWatched(targetPid, vnMsgId);
  if(wsUp){
    wsSend({type:'vn-watched', target:targetPid, vnMsgId});
    // Если WS работает — убираем из очереди сразу (оптимизация)
    const q = lsGet(VN_WATCHED_QUEUE_KEY, []);
    const filtered = q.filter(x=>!(x.targetPid===targetPid&&x.vnMsgId===vnMsgId));
    try{lsSet(VN_WATCHED_QUEUE_KEY, filtered);}catch(e){}
  }
}

// Главная функция: отправить voice-listened (с offline-гарантией)
function sendVoiceListened(targetPid, voiceMsgId){
  if(!targetPid || targetPid === MY_ID) return;
  _enqueueVoiceListened(targetPid, voiceMsgId);
  if(wsUp){
    wsSend({type:'voice-listened', target:targetPid, voiceMsgId});
    const q = lsGet(VOICE_LISTENED_QUEUE_KEY, []);
    const filtered = q.filter(x=>!(x.targetPid===targetPid&&x.voiceMsgId===voiceMsgId));
    try{lsSet(VOICE_LISTENED_QUEUE_KEY, filtered);}catch(e){}
  }
}
async function syncDeliveredStatuses(){if(!activePid)return;const msgs=await loadMsgs(activePid);for(const m of msgs){if(m.type==='sent'&&m.delivered){const row=document.querySelector(`.msg-row[data-msgid="${m.id}"]`);if(row){const tk=row.querySelector('.msg-ticks');if(tk&&tk.innerHTML!=='<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M18 6 7 17l-5-5" /> <path d="m22 10-7.5 7.5L13 16" /> </svg>'){tk.innerHTML='<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M18 6 7 17l-5-5" /> <path d="m22 10-7.5 7.5L13 16" /> </svg>';tk.className='msg-ticks double';}}}if(m.type==='sent'&&m.voice&&m.voiceListened){const row=document.querySelector(`.msg-row[data-msgid="${m.id}"]`);if(row){const dot=row.querySelector('.voice-unread-dot');if(dot)dot.classList.add('hidden');}}if(m.type==='sent'&&m.videoNote&&m.voiceListened){const row=document.querySelector(`.msg-row[data-msgid="${m.id}"]`);if(row){const dot=row.querySelector('.vn-unread-dot');if(dot)dot.classList.add('hidden');const badge=row.querySelector('.vn-circle-unread-badge');if(badge)badge.classList.add('hidden');}}if(m.type==='recv'&&m.videoNote&&m.voiceListened){const row=document.querySelector(`.msg-row[data-msgid="${m.id}"]`);if(row){const dot=row.querySelector('.vn-unread-dot');if(dot)dot.classList.add('hidden');const badge=row.querySelector('.vn-circle-unread-badge');if(badge)badge.classList.add('hidden');}}}}

const online={};

async function onWS(msg){
  if(msg.type==='incoming-msg'&&msg.from===MY_ID)return;
  
  // УЛЬТИМАТИВНАЯ ОПТИМИЗАЦИЯ: Фиксация ID события для Delta-Sync
  const eventId=msg.eventId;
  if (eventId && eventId > lsGet('bc_last_event_id', 0)) {
    lsSet('bc_last_event_id', eventId);
  }
  switch(msg.type){
    case 'registered':
      updateSendBtn();
      if(activePid) wsSend({type:'query-presence',target:activePid});
      registerPushNotifications().catch(()=>{});
      broadcastLastSeen().catch(()=>{});
      
      // УЛЬТИМАТИВНАЯ ОПТИМИЗАЦИЯ: Delta-Sync (запрос пропущенных событий)
      // Мы сообщаем серверу ID последнего полученного события, чтобы он дослал только новое.
      const lastEvId = lsGet('bc_last_event_id', 0);
      wsSend({ type: 'delta-sync', sinceId: lastEvId });

      // После reconnect — drain все pending сообщения у которых есть ключ
      _drainAllPendingOnReconnect().catch(()=>{});
      // Flush ВСЕ pending ack'и
      _flushAllPendingAcks();
      // Отправляем накопленные vn-watched и voice-listened
      _drainWatchedQueues();
      // Если чат сейчас открыт — повторяем chat-read
      if(activePid && currentScreen==='scr-chat') _sendChatReadReceipt(activePid).catch(()=>{});
      // Возобновляем загрузки файлов
      _resumeAllFileTransfers();
      // ОПТИМИЗАЦИЯ 10: _drainOutbox при reconnect
      _drainOutbox().catch(()=>{});
      // Дренируем wsSend-буфер
      if (window._wsSendBuffer.length > 0) {
        const buf = window._wsSendBuffer.splice(0);
        const now = Date.now();
        for (const item of buf) {
          if (now - item.ts < OUTBOX_TTL) wsSend(item.obj);
        }
      }
      break;
    case 'presence':setOnline(msg.peerId.toLowerCase(),!!msg.online,true);break;
    case 'presence-reply':setOnline((msg.target||'').toLowerCase(),!!msg.online,false);break;
    case 'file-available':if(!isContactBlocked((msg.senderId||'').toLowerCase())){await handleFileAvailable(msg);}if(eventId)wsSend({type:'ack-event',eventId});break;
    // ОПТИМИЗАЦИЯ 3+4: сервер сообщает сколько чанков уже есть — обновляем UI
    case 'file-chunks-update':{
      const{fileId:cuFileId,chunksReady:cuReady,totalChunks:cuTotal}=msg;
      const ft=fileTransfers.get(cuFileId);
      if(ft){
        ft._chunksReady = cuReady;
        if(ft.state==='downloading'){
          const pct=Math.round((cuReady/cuTotal)*100);
          updateSpinnerProgress(cuFileId,pct);
          // Если мы "зависли" на ожидании чанков, а они появились — толкаем загрузку
          if(ft.received >= (ft._lastReceivedIndex||0) && ft.received < cuTotal){
            _retryDownload(cuFileId);
          }
        }
      }
      break;
    }
    case 'file-data-header':await handleFileDataHeader(msg);break;
    case 'file-data-chunk':await handleFileDataChunk(msg);break;
    case 'store-file-header-ack':{const waiter=storeAckWaiters.get(msg.fileId);if(waiter)waiter.resolve();break;}
    case 'store-chunks-ack':
      if(storeAckWaiters.has(msg.fileId)) storeAckWaiters.get(msg.fileId).resolve();
      if(msg.receivedCount !== undefined){
        const lastIdx = msg.receivedCount - 1;
        if(storeAckWaiters.has('chunk_'+msg.fileId+'_'+lastIdx)) storeAckWaiters.get('chunk_'+msg.fileId+'_'+lastIdx).resolve();
      }
      break;
    case 'file-status':
      if(storeAckWaiters.has('status_'+msg.fileId)) storeAckWaiters.get('status_'+msg.fileId).resolve(msg);
      break;
    case 'file-upload-complete':{
      // Сервер получил все чанки — финализируем UI
      const {fileId:fucId}=msg;
      // Отменяем fallback таймер если есть
      if(window._uploadFinalizeTimers?.[fucId]){
        clearTimeout(window._uploadFinalizeTimers[fucId]);
        delete window._uploadFinalizeTimers[fucId];
      }
      await finalizeUploadUI(fucId,activePid);
      break;
    }
    case 'file-fetch-partial':{
      // Файл ещё не полностью загружен на сервер — повторим попытку через 5с
      const {fileId:pfId,received:pfRec,total:pfTotal}=msg;
      console.warn(`[File] Partial on server: ${pfId} ${pfRec}/${pfTotal}, retry in 5s`);
      toast('Файл ещё загружается на сервер, подождите…','warn');
      setTimeout(()=>{
        const ft=fileTransfers.get(pfId);
        if(ft)wsSend({type:'fetch-file',fileId:pfId});
      },5000);
      break;
    }
    case 'file-fetch-error':{
      const {fileId:efId,msg:efMsg}=msg;
      console.error(`[File] Fetch error: ${efId}`, efMsg);
      toast(`Ошибка загрузки файла: ${efMsg}`,'err');
      // Сбрасываем спиннер обратно в кнопку Download
      const spinner=document.querySelector(`.tg-spinner[data-fileid="${efId}"]`);
      if(spinner){
        const ft=fileTransfers.get(efId);
        resetSpinnerToDownload(spinner,efId,ft?.senderId||'');
        fileTransfers.delete(efId);
      }
      break;
    }
    case 'file-delivered':{
      const {fileId:fdFileId, recipientId:fdRecipient} = msg;
      // Определяем peerId: сначала из нашего Map, потом из поля recipientId от сервера
      const fdPeerId = _fileSentToPeer.get(fdFileId) || (fdRecipient||'').toLowerCase() || null;
      if(fdFileId && fdPeerId){
        _fileSentToPeer.delete(fdFileId); // чистим Map
        try{const _sftp=lsGet('_fileSentToPeer',{});delete _sftp[fdFileId];lsSet('_fileSentToPeer',_sftp);}catch(e){}
        await markDelivered(fdFileId, fdPeerId, true);
      }
      if(eventId) wsSend({type:'ack-event',eventId});
      break;
    }
    case 'incoming-msg':{
      const from=(msg.from||'').toLowerCase();
      // НЕ отправляем ack-msg «навсегда в память» — сначала надёжно
      // кладём в persistent-очередь, а уже потом flush'им если чат открыт.
      // Это защищает read-receipts при reconnect / leaveChat / reload.
      _queuePendingAck(from,msg.msgId,eventId);
      // Если я СЕЙЧАС нахожусь в чате с этим человеком — отправляем сразу
      // (я уже вижу сообщение на экране)
      if(activePid === from && currentScreen === 'scr-chat'){
        _flushPendingAcks(from);
      }
      // Если контакт заблокирован — молча игнорируем входящее сообщение
      if(isContactBlocked(from)){
        // Всё равно ackуем чтобы сервер не держал событие в очереди.
        // eventId уже лежит внутри pending-ack и будет отправлен вместе с ack-msg.
        _flushPendingAcks(from);
        break;
      }
      // Немедленно пробуем drain pending для этого отправителя (если есть ключ)
      getKey(from).then(k=>{if(k)drainPending(from,k).catch(()=>{});}).catch(()=>{});
      let key=keyCache[from]||await loadPersistedKey(from);
      if(!key){const pwd=localStorage.getItem(`bc_pwd_${from}`)||sessionStorage.getItem(`bc_pwd_${from}`);if(pwd){try{key=await deriveKey(pwd,from);keyCache[from]=key;await persistKey(from,key);}catch(e){}}}
      if(!key){const pending=lsGet(`bc_pending_${from}`,[]);if(!pending.some(p=>p.msgId===msg.msgId))pending.push({msgId:msg.msgId,payload:msg.payload,ts:Date.now()});lsSet(`bc_pending_${from}`,pending);const ct=loadContacts().find(c=>c.id===from);await getOrCreateChat(from,ct?.name,ct?.avatar);const chat=(await loadChats()).find(c=>c.peerId===from);await updateChat(from,{unread:(chat?.unread||0)+1,lastMsg:'Зашифровано',lastMsgTime:Date.now()});toast(`${ct?.name||from}: (зашифровано)`);break;}
      try{const buf=typeof msg.payload==='string'?base64ToArrayBuffer(msg.payload):new Uint8Array(msg.payload).buffer;const pt=DEC.decode(await decData(buf,key));await handleEnvelope(from,JSON.parse(pt));}catch(e){console.warn('decrypt fail',e);}
      break;
    }
    case 'msg-delivered':await markDelivered(msg.msgId,msg.by);if(eventId)wsSend({type:'ack-event',eventId});break;
    case 'voice-listened':await markVoiceListened(msg.voiceMsgId,(msg.from||'').toLowerCase());if(eventId)wsSend({type:'ack-event',eventId});break;
    case 'vn-watched':await markVnWatched(msg.vnMsgId,(msg.from||'').toLowerCase());if(eventId)wsSend({type:'ack-event',eventId});break;
  }
}

async function decryptAndStore(from,payload,key){try{const buf=typeof payload==='string'?base64ToArrayBuffer(payload):new Uint8Array(payload).buffer;const pt=DEC.decode(await decData(buf,key));await handleEnvelope(from,JSON.parse(pt));}catch(e){console.warn('decrypt fail',e);}}

async function handleEnvelope(from,env){
  if(env.type==='sticker'){
    const m={id:env.id,sticker:env.sticker,type:'recv',time:new Date(env.ts||Date.now()).toISOString(),reactions:{},edited:false,replyTo:env.replyTo||null,delivered:true,forwarded:false};
    const ct=loadContacts().find(co=>co.id===from);
    await getOrCreateChat(from,ct?.name,ct?.avatar);
    // Дедупликация
    const _existSticker=await loadMsgs(from);
    if(_existSticker.some(x=>x.id===m.id)){
      if(currentScreen==='scr-chat'&&activePid===from){
        const existingRow=document.querySelector(`.msg-row[data-msgid="${m.id}"]`);
        if(!existingRow)appendOrReloadMsg(from,m);
      }
      return;
    }
    await upsertMsg(from,m);
    await updateChat(from,{lastMsg:'Стикер',lastMsgTime:new Date(m.time).getTime()});
    if(currentScreen==='scr-chat'&&activePid===from){
      appendOrReloadMsg(from,m);
      if(from===activePid){stopPeerActivity();updateBar();}
    }else{
      const chat=(await loadChats()).find(ch=>ch.peerId===from);
      await updateChat(from,{unread:(chat?.unread||0)+1,marked:false});
      toast((chat?.peerName||from)+': Стикер');
    }
    return;
  }
  if(env.type==='msg'||env.type==='media'||env.type==='voice'||env.type==='video_note'){
    const m={id:env.id,text:env.text||'',type:'recv',time:new Date(env.ts||Date.now()).toISOString(),reactions:{},edited:false,replyTo:env.replyTo||null,delivered:true,forwarded:env.forwarded||false};
    if(env.type==='media'){
      if(env.media?.data){
        // Сначала сохраняем с data (для немедленного показа), потом мигрируем в blob
        m.media={type:env.media.type,data:env.media.data};
        m.caption=env.caption;
        // Сохраняем в blobs асинхронно в фоне — не блокируем UI
        const _mediaData=env.media.data;
        const _mediaId=env.id+'_media';
        const _mediaType=env.media.type;
        Promise.resolve().then(async()=>{
          try{
            const buf=base64ToArrayBuffer(_mediaData.replace(/^data:[^,]+,/,''));
            await saveBlob(_mediaId,buf);
            // Обновляем запись: убираем data, добавляем blobId
            await withChatLock(m.id.split('_')[0]||env.id,async()=>{
              const pid=loadContacts().find(c=>c.id===env.id)||{id:env.id};
              // Обновляем в хранилище без перерендера
            });
          }catch(e){}
        }).catch(()=>{});
      }else{m.media=env.media;m.caption=env.caption;}
    }
    else if(env.type==='voice'){
      if(env.voice?.data){
        // Немедленно показываем с data, потом мигрируем в blob
        m.voice={waveform:env.voice.waveform,data:env.voice.data};
        m.voiceDuration=env.voiceDuration;m.voiceListened=false;
        const _voiceData=env.voice.data;
        const _voiceId=env.id+'_voice';
        Promise.resolve().then(async()=>{
          try{
            const buf=base64ToArrayBuffer(_voiceData.replace(/^data:[^,]+,/,''));
            await saveBlob(_voiceId,buf);
          }catch(e){}
        }).catch(()=>{});
      }else{m.voice=env.voice;m.voiceDuration=env.voiceDuration;m.voiceListened=false;}
    }
    else if(env.type==='video_note'){
      // Видео-кружок: data URL → сразу сохраняем blob, показываем через videoData
      m.videoNote=true;
      m.voiceDuration=env.voiceDuration||0;
      m.voiceListened=false;
      if(env.videoData){
        // Сначала сохраняем blob синхронно (await), потом рендерим
        const _vid=env.id+'_vnote';
        try{
          const buf=base64ToArrayBuffer(env.videoData.replace(/^data:[^,]+,/,''));
          await saveBlob(_vid,buf);
          // В БД храним только blobId (без videoData — слишком большой)
          m.videoBlobId=_vid;
          m.videoMime=env.videoMime||'video/webm';
          // Для рендера добавляем videoData временно (не пойдёт в БД)
          m._videoDataForRender=env.videoData;
        }catch(e){
          // Если не удалось сохранить — используем dataURL напрямую (запасной вариант)
          m.videoData=env.videoData;
          m.videoMime=env.videoMime||'video/webm';
        }
      } else if(env.videoBlobId){
        m.videoBlobId=env.videoBlobId;
        m.videoMime=env.videoMime||'video/webm';
      }
    }
    const ct=loadContacts().find(c=>c.id===from);await getOrCreateChat(from,ct?.name,ct?.avatar);
    // ── Дедупликация: проверяем нет ли уже такого сообщения в базе ──
    const _existingMsgs=await loadMsgs(from);
    const _isDup=_existingMsgs.some(x=>x.id===m.id);
    if(_isDup){
      // Дубль — только обновляем DOM если нужно, счётчик не трогаем
      if(currentScreen==='scr-chat'&&activePid===from){
        const existingRow=document.querySelector(`.msg-row[data-msgid="${m.id}"]`);
        if(!existingRow)appendOrReloadMsg(from,m);
      }
      return;
    }
    await upsertMsg(from,m);
    const lastPreview=env.type==='voice'?'Голосовое':env.type==='video_note'?'Видео-кружок':env.type==='media'?'Фото':(env.text||'').slice(0,28);
    await updateChat(from,{lastMsg:lastPreview,lastMsgTime:new Date(m.time).getTime()});
    if(currentScreen==='scr-chat'&&activePid===from){
      appendOrReloadMsg(from,m);
      // Сбрасываем статус активности при получении реального сообщения
      if(from===activePid){stopPeerActivity();updateBar();}
    }else{const chat=(await loadChats()).find(c=>c.peerId===from);await updateChat(from,{unread:(chat?.unread||0)+1,marked:false});toast(`${chat?.peerName||from}: ${lastPreview}`);}
  }else if(env.type==='file-ref'){
    if(from===activePid&&currentScreen==='scr-chat'){stopPeerActivity();updateBar();}
    await handleFileRef(from,env);
  }else if(env.type==='reaction'){
    await handleInReaction(from,env.msgId,env.emoji);
  }else if(env.type==='edit'){
    await handleInEdit(from,env.msgId,env.newText,env.ts);
  }else if(env.type==='delete'){
    await handleInDelete(from,env.msgId);
  }else if(env.type==='typing'){
    if(from===activePid&&currentScreen==='scr-chat')handleTyping(env.isTyping);
  }else if(env.type==='last-seen'){
    // Получили time last seen от собеседника — значит он включил эту функцию
    if(env.ts && typeof env.ts === 'number'){
      // Принимаем только если НОВЕЕ сохранённого (защита от старых очередных сообщений)
      const storedTs = lsGet(`bc_last_seen_${from}`, 0) || 0;
      if(env.ts > storedTs){
        lsSet(`bc_last_seen_${from}`, env.ts);
      }
      lsSet(`bc_ls_allowed_${from}`, true);
      if(from===activePid && currentScreen==='scr-chat' && !online[from]){
        updateBar();
      }
    }
  }else if(env.type==='last-seen-off'){
    // Собеседник выключил функцию — скрываем его lastSeen
    lsSet(`bc_ls_allowed_${from}`, false);
    if(from===activePid && currentScreen==='scr-chat' && !online[from]){
      updateBar();
    }
  }else if(env.type==='block-status'){
    // Собеседник заблокировал / разблокировал нас
    const wasBlockedByPeer = lsGet(`bc_blocked_by_${from}`, false);
    lsSet(`bc_blocked_by_${from}`, !!env.blocked);
    if(from===activePid && currentScreen==='scr-chat'){
      if(env.blocked){
        // Нас заблокировали — показываем "вы заблокированы" и блокируем ввод
        setPeerStatus('blocked','Вы заблокированы');
        const inputBar=$('chatInputBar');
        if(inputBar)inputBar.classList.add('blocked-mode');
        $('sendBtn').disabled=true;
        const banner=$('blockedBanner');
        if(banner){banner.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M4.929 4.929 19.07 19.071\" /> </svg> Вы заблокированы этим контактом';banner.classList.add('visible');}
      }else{
        // Нас разблокировали — восстанавливаем
        const inputBar=$('chatInputBar');
        if(inputBar)inputBar.classList.remove('blocked-mode');
        const banner=$('blockedBanner');
        if(banner){banner.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M4.929 4.929 19.07 19.071\" /> </svg> Контакт заблокирован — отправка сообщений недоступна';banner.classList.remove('visible');}
        updateBar();
        updateSendBtn();
      }
    }
  }else if(env.type==='activity'){
    // Heartbeat-статус от собеседника: каждый новый сигнал сбрасывает таймер на 5с
    if(from===activePid&&currentScreen==='scr-chat'){
      if(env.activityType){
        startPeerActivity(env.activityType); // внутри уже сбрасывает 5с таймер
      }else{stopPeerActivity();updateBar();}
    }
  }else if(env.type==='file-delivered'){
    // Получатель скачал наш файл — ставим ✔✔
    if(env.fileId){
      await markDelivered(env.fileId, from, true);
    }
  }else if(env.type==='chat-read'){
    // Получатель открыл чат и прочитал наши сообщения → ставим ✔✔
    if(Array.isArray(env.msgIds) && env.msgIds.length){
      for(const mid of env.msgIds){
        await markDelivered(mid, from, true);
      }
    }
  }else if(env.type==='pin-msg'){
    await handleInPinMsg(from,env);
  }else if(env.type==='autodel-set'){
    await handleInAutoDelSet(from,env);
  }
}

// Drain pending для всех чатов при reconnect (вызывается после registered)
async function _drainAllPendingOnReconnect(){
  // Небольшая задержка — даём серверу время прислать очередные incoming-msg
  await new Promise(r=>setTimeout(r,800));
  const chats = await loadChats();
  for(const chat of chats){
    if(chat.peerId===MY_ID) continue;
    // Drain bc_pending_ если ключ уже в кэше или в localStorage
    const key = await getKey(chat.peerId).catch(()=>null);
    if(key){
      await drainPending(chat.peerId, key).catch(()=>{});
    }
  }
}

// Отправляем собеседнику зашифрованный read-receipt
// Он получит его и поставит ✔✔ на своих отправленных сообщениях
async function _sendChatReadReceipt(pid){
  if(pid===MY_ID) return;
  const ok = await waitForWS(5000);
  if(!ok || !wsUp) return;
  const key = keyCache[pid] || await loadPersistedKey(pid);
  if(!key) return;
  // Собираем ID всех НЕ прочитанных входящих сообщений от этого пира
  const msgs = await loadMsgs(pid);
  // Отправляем один envelope со списком msgId входящих сообщений
  // Собеседник пометит их как delivered (✔✔ у него)
  // ИСПРАВЛЕНИЕ: исключаем файлы и видео-кружки, которые ещё не скачаны (fileAvailable / _vnPending).
  // Для них ✔✔ ставится только через file-delivered — после реального скачивания файла получателем.
  const unreadIds = msgs
    .filter(m => m.type==='recv' && !m._readReceiptSent && !m.fileAvailable && !(m.videoNote && m._vnPending))
    .map(m => m.id);
  if(!unreadIds.length) return;
  try{
    const enc = await encData(ENC.encode(JSON.stringify({
      type:'chat-read', msgIds:unreadIds, ts:Date.now()
    })), key);
    if(!wsUp) return;
    wsSend({type:'send-msg', target:pid, msgId:uid(), payload:payloadToB64(enc)});
    // Помечаем как отправленные ТОЛЬКО после успешной отправки в живой WS.
    await withChatLock(pid, async()=>{
      const m2 = await loadMsgs(pid);
      let changed = false;
      const unreadSet = new Set(unreadIds);
      for(const m of m2){
        if(m.type==='recv' && unreadSet.has(m.id) && !m._readReceiptSent){
          m._readReceiptSent = true;
          changed = true;
        }
      }
      if(changed) await saveMsgs(pid, m2);
    });
  }catch(e){}
}

async function drainPending(pid,key){const pending=lsGet(`bc_pending_${pid}`,[]);if(!pending.length)return;localStorage.removeItem(`bc_pending_${pid}`);for(const p of pending)await decryptAndStore(pid,p.payload,key);}

// ── PRESENCE ──
function setOnline(pid, isOnline, isRealEvent){
  // isRealEvent=true  → 'presence' — сервер пушит событие выхода/входа прямо сейчас
  // isRealEvent=false → 'presence-reply' — ответ на наш запрос текущего статуса
  //
  // КРИТИЧЕСКИ ВАЖНО: bc_last_seen_* пишем ТОЛЬКО при isRealEvent=true && !isOnline
  // Иначе каждый query-presence (presence-reply: offline) будет перезаписывать
  // last-seen текущим временем → всегда "только что"
  const wasOnline = online[pid];
  online[pid] = isOnline;

  if(!isOnline && isRealEvent){
    // Контакт вышел из сети прямо сейчас — записываем точный timestamp
    lsSet(`bc_last_seen_${pid}`, Date.now());
  }
  // presence-reply: если offline — НЕ обновляем bc_last_seen_*
  // (не знаем когда он реально вышел, реальный ts придёт через last-seen envelope)

  if(pid===activePid && currentScreen==='scr-chat') updateBar();
  renderChatList(); renderContacts();
}
// ── СТАТУС СОЕДИНЕНИЯ В ШАПКЕ (Telegram-стиль) ──
// Анимация точек для статусов «Подключение» / «Переподключение» / «Соединение»
let _connStatusInterval=null;
let _connStatusDots=1;
let _connStatusBase='';

function _startConnDots(base){
  _stopConnDots();
  _connStatusBase=base;
  _connStatusDots=1;
  const el=$('peerStatus');
  const homeHeader=$('homeHeaderTitle');
  function render(){
    const text = _connStatusBase+'.'.repeat(_connStatusDots);
    if(el){
      el.textContent=text;
      el.className='chat-peer-status connecting';
    }
    if(homeHeader){
      homeHeader.textContent=text;
      homeHeader.classList.add('connecting');
    }
    _connStatusDots=_connStatusDots>=3?1:_connStatusDots+1;
  }
  render();
  _connStatusInterval=setInterval(render,500);
}

function _stopConnDots(){
  if(_connStatusInterval){clearInterval(_connStatusInterval);_connStatusInterval=null;}
  _connStatusBase='';
  _connStatusDots=1;
  const homeHeader=$('homeHeaderTitle');
  if(homeHeader){
    homeHeader.textContent='K-Chat';
    homeHeader.classList.remove('connecting');
  }
}

// setBar теперь — no-op (statusBar убран), оставляем для совместимости
function setBar(_cls,_txt){}

function setPeerStatus(cls,txt){
  // Если устанавливаем не-connecting статус — останавливаем анимацию точек
  if(cls!=='connecting')_stopConnDots();
  const el=$('peerStatus');
  if(!el)return;
  el.textContent=txt;
  el.className='chat-peer-status '+cls;
}

function updateBar(){
  if(_peerActivityType)return; // не перебиваем анимированный статус активности
  if(!wsUp){
    // Нет связи — «Соединение...» с анимацией точек
    _stopConnDots();
    _startConnDots('Соединение');
    return;
  }
  if(!activePid)return;
  // Если контакт заблокирован нами
  if(isContactBlocked(activePid)){setPeerStatus('blocked','Контакт заблокирован');return;}
  // Если нас заблокировал собеседник
  if(lsGet(`bc_blocked_by_${activePid}`,false)){setPeerStatus('blocked','Вы заблокированы');return;}
  if(online[activePid]){
    _stopConnDots();
    setPeerStatus('online','В сети');
  } else {
    _stopConnDots();
    const lastSeenTs=lsGet(`bc_last_seen_${activePid}`,null);
    const lastSeenAllowed=lsGet(`bc_ls_allowed_${activePid}`,false);
    if(lastSeenTs&&lastSeenAllowed){
      setPeerStatus('offline',fmtLastSeen(lastSeenTs));
    }else{
      setPeerStatus('offline','Не в сети');
    }
  }
}
function fmtLastSeen(ts){
  if(!ts) return 'Не в сети';
  const now = new Date();
  const d   = new Date(ts);
  const diffMs  = now - d;
  const diffMin = Math.floor(diffMs / 30000);
  const diffH   = Math.floor(diffMs / 3300000);

  // Менее минуты назад
  if(diffMin < 1) return 'только что в сети';

  // Менее часа
  if(diffMin < 60){
    const m = diffMin;
    const word = m===1?'минуту':m<5?'минуты':'минут';
    return `был(а) ${m} ${word} назад`;
  }

  // Сегодня
  const nowDay   = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const dDay     = new Date(d.getFullYear(),   d.getMonth(),   d.getDate());
  const dayDiff  = Math.round((nowDay - dDay) / 86400000);

  const timeStr  = d.toLocaleTimeString('ru', {hour:'2-digit', minute:'2-digit'});

  if(dayDiff === 0) return `был(а) в ${timeStr}`;
  if(dayDiff === 1) return `был(а) вчера в ${timeStr}`;

  // Этот год
  const thisYear = now.getFullYear();
  const months   = ['янв.','февр.','марта','апр.','мая','июня','июля','авг.','сент.','окт.','нояб.','дек.'];
  const dayNum   = d.getDate();
  const mon      = months[d.getMonth()];

  if(d.getFullYear() === thisYear){
    return `был(а) ${dayNum} ${mon} в ${timeStr}`;
  }

  // Прошлый год и ранее
  return `был(а) ${dayNum} ${mon} ${d.getFullYear()} г. в ${timeStr}`;
}

function updateSendBtn(){
  if(!activePid){$('sendBtn').disabled=true;return;}
  if(isContactBlocked(activePid)||lsGet(`bc_blocked_by_${activePid}`,false)){$('sendBtn').disabled=true;updateBar();return;}
  // Улучшение для 2G/3G: Кнопка отправки ВСЕГДА активна, если есть текст.
  // Если интернета нет (!wsUp), сообщение уйдет в Outbox автоматически.
  $('sendBtn').disabled=false;
  updateBar();
}

// ── CHATS ──
async function getOrCreateChat(pid,peerName,avatar){let chats=await loadChats();let c=chats.find(c=>c.peerId===pid);if(!c){c={peerId:pid,peerName:peerName||(pid===MY_ID?'⭐ Избранное':pid),avatar:avatar||(pid===MY_ID?'⭐':'👤'),lastMsg:null,lastMsgTime:null,unread:0};chats.push(c);}else{if(peerName)c.peerName=peerName;if(avatar)c.avatar=avatar;}await saveChats(chats);return c;}
async function updateChat(pid,upd){let c=await loadChats();const i=c.findIndex(x=>x.peerId===pid);if(i!==-1){c[i]={...c[i],...upd};await saveChats(c);}renderChatList();}
async function deleteChatData(pid){await saveChats((await loadChats()).filter(c=>c.peerId!==pid));renderChatList();}
window.clearAllData=()=>{ showConfirm({icon:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M21 21H8a2 2 0 0 1-1.42-.587l-3.994-3.999a2 2 0 0 1 0-2.828l10-10a2 2 0 0 1 2.829 0l5.999 6a2 2 0 0 1 0 2.828L12.834 21\" /> <path d=\"m5.082 11.09 8.828 8.828\" /> </svg>',title:'Стереть все данные?',text:'Все чаты, история, контакты и ключи шифрования будут удалены навсегда.',yesLabel:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M21 21H8a2 2 0 0 1-1.42-.587l-3.994-3.999a2 2 0 0 1 0-2.828l10-10a2 2 0 0 1 2.829 0l5.999 6a2 2 0 0 1 0 2.828L12.834 21" /> <path d="m5.082 11.09 8.828 8.828" /> </svg> Да, стереть всё',onYes:()=>{_chatsCache=null;localStorage.clear();indexedDB.deleteDatabase(DB_NAME);setTimeout(()=>location.reload(),200);}});};

// ── TABS / FAB ──
let currentTab='chats';
window.setTab=t=>{currentTab=t;$('tabChats').classList.toggle('active',t==='chats');$('tabGroups').classList.toggle('active',t==='groups');renderChatList();};
let fabOpen=false;
window.toggleFab=()=>{fabOpen=!fabOpen;$('fabMenu').classList.toggle('open',fabOpen);$('fabMain').classList.toggle('open',fabOpen);};
function closeFab(){fabOpen=false;$('fabMenu').classList.remove('open');$('fabMain').classList.remove('open');}
document.addEventListener('click',e=>{if(fabOpen&&!$('fabWrap').contains(e.target))closeFab();});
window.onNewChat=()=>{closeFab();_openChatAfterSave=true;openAddContact();};
window.onNewGroup=()=>{closeFab();toast('Создание групп скоро появится','warn');};
// ── THROTTLE — ограничиваем частоту scroll-событий в power-save ──────────────
function _throttle(fn, ms){
  let last=0, raf=null;
  return function(...args){
    const now=Date.now();
    if(now-last >= ms){
      last=now;
      fn.apply(this,args);
    } else if(!raf){
      raf=setTimeout(()=>{raf=null;last=Date.now();fn.apply(this,args);}, ms-(now-last));
    }
  };
}

function _onMessagesScroll(){updateScrollBtn();showFloatingDate();}
let _scrollHandler=_onMessagesScroll;
function _rebuildScrollHandler(){
  const area=$('messagesArea');
  if(!area)return;
  area.removeEventListener('scroll',_scrollHandler);
  _scrollHandler = lsGet('bc_energy_save',false)
    ? _throttle(_onMessagesScroll, 80)
    : _onMessagesScroll;
  area.addEventListener('scroll',_scrollHandler);
}

// ── ЭНЕРГОСБЕРЕЖЕНИЕ ──────────────────────────────────────────────────────────
function applyEnergySave(on){
  document.body.classList.toggle('power-save', !!on);
  document.querySelectorAll('.bg-orb').forEach(o=>{
    o.style.display = on ? 'none' : '';
  });
  _rebuildScrollHandler();
}

// Применяем при старте
applyEnergySave(lsGet('bc_energy_save', false));

window.openSettings=()=>openSettingsModal();
window.closeSettingsModal=()=>closeSettingsModal();

// ══════════════════════════════════════════════════════
// ── ТЕМЫ / АКЦЕНТНЫЕ ЦВЕТА ──
// ══════════════════════════════════════════════════════
const THEMES = [
  {id:'green',   label:'Зелёный',  g:'#00ff88', b:'#00d9ff', grad:'linear-gradient(135deg,#00ff88,#00d9ff)'},
  {id:'blue',    label:'Синий',    g:'#4d9fff', b:'#00cfff', grad:'linear-gradient(135deg,#4d9fff,#00cfff)'},
  {id:'purple',  label:'Фиолет.',  g:'#b06aff', b:'#e040fb', grad:'linear-gradient(135deg,#b06aff,#e040fb)'},
  {id:'red',     label:'Красный',  g:'#ff4f6a', b:'#ff2244', grad:'linear-gradient(135deg,#ff4f6a,#ff2244)'},
  {id:'orange',  label:'Оранж.',   g:'#ffa62b', b:'#ff6f00', grad:'linear-gradient(135deg,#ffa62b,#ff6f00)'},
  {id:'yellow',  label:'Жёлтый',   g:'#ffe033', b:'#ffb300', grad:'linear-gradient(135deg,#ffe033,#ffb300)'},
  {id:'pink',    label:'Розовый',  g:'#ff6eb4', b:'#ff3d9a', grad:'linear-gradient(135deg,#ff6eb4,#ff3d9a)'},
  {id:'cyan',    label:'Голубой',  g:'#06B6D4', b:'#0ea5e9', grad:'linear-gradient(135deg,#06B6D4,#0ea5e9)'},
  {id:'teal',    label:'Бирюза',   g:'#14B8A6', b:'#06b6d4', grad:'linear-gradient(135deg,#14B8A6,#06b6d4)'},
  {id:'indigo',  label:'Индиго',   g:'#6366F1', b:'#8B5CF6', grad:'linear-gradient(135deg,#6366F1,#8B5CF6)'},
  {id:'rose',    label:'Роза',     g:'#F43F5E', b:'#fb7185', grad:'linear-gradient(135deg,#F43F5E,#fb7185)'},
  {id:'emerald', label:'Изумруд',  g:'#059669', b:'#10b981', grad:'linear-gradient(135deg,#059669,#10b981)'},
];

function applyTheme(themeId){
  const t = THEMES.find(x=>x.id===themeId) || THEMES[0];
  const r = document.documentElement.style;
  r.setProperty('--g', t.g);
  r.setProperty('--b', t.b);
  r.setProperty('--border',    hexToRgba(t.g, 0.12));
  r.setProperty('--border-hi', hexToRgba(t.g, 0.35));
  // Все alpha-версии акцентного цвета
  const alphas = [
    ['--g-06',.06],['--g-08',.08],['--g-10',.10],['--g-12',.12],
    ['--g-14',.14],['--g-15',.15],['--g-18',.18],['--g-20',.20],
    ['--g-25',.25],['--g-28',.28],['--g-30',.30],['--g-35',.35],
    ['--g-40',.40],['--g-85',.85],['--g-glow',.70],
  ];
  alphas.forEach(([name,a]) => r.setProperty(name, hexToRgba(t.g, a)));
  // --g-rgb для использования в rgba(var(--g-rgb), alpha)
  const _hex=t.g;
  const _rr=parseInt(_hex.slice(1,3),16);
  const _gg=parseInt(_hex.slice(3,5),16);
  const _bb=parseInt(_hex.slice(5,7),16);
  r.setProperty('--g-rgb', `${_rr},${_gg},${_bb}`);
  // Фон пузыря отправителя — затемнённый акцентный цвет
  const bubbleBg = `linear-gradient(135deg,${hexToRgba(t.g,.45)},${hexToRgba(t.g,.28)})`;
  const bubbleBorder = hexToRgba(t.g, 0.5);
  r.setProperty('--bubble-sent-bg', bubbleBg);
  r.setProperty('--bubble-sent-border', bubbleBorder);
  // Перекрашиваем bg-orbs
  const orb1 = document.querySelector('.bg-orb-1');
  if(orb1) orb1.style.background = `radial-gradient(circle,${hexToRgba(t.g,0.07)} 0%,transparent 70%)`;
  const orb2 = document.querySelector('.bg-orb-2');
  if(orb2) orb2.style.background = `radial-gradient(circle,${hexToRgba(t.b,0.06)} 0%,transparent 70%)`;
  // Обновляем body::before (сетка) через CSS custom property не работает,
  // поэтому меняем через style на body
  document.body.style.setProperty('--grid-color', hexToRgba(t.g, 0.025));
  lsSet('bc_theme', themeId);
}

function hexToRgba(hex, alpha){
  const r=parseInt(hex.slice(1,3),16);
  const g=parseInt(hex.slice(3,5),16);
  const b=parseInt(hex.slice(5,7),16);
  return `rgba(${r},${g},${b},${alpha})`;
}

function loadSavedTheme(){
  const saved = lsGet('bc_theme','green');
  applyTheme(saved);
}

// ══════════════════════════════════════════════════════
// ── КАСТОМНЫЙ CONFIRM ──
// ══════════════════════════════════════════════════════
// Склонение слова "сообщение" по числительному (1 сообщение / 2 сообщения / 5 сообщений)
function _pluralMsgWord(n){
  const mod10=n%10, mod100=n%100;
  if(mod10===1 && mod100!==11) return 'сообщение';
  if(mod10>=2 && mod10<=4 && (mod100<12||mod100>14)) return 'сообщения';
  return 'сообщений';
}

function showConfirm({icon='<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3" /> <path d="M12 9v4" /> <path d="M12 17h.01" /> </svg>', title, text, yesLabel='Да', noLabel='Отмена', onYes}){
  const overlay = $('confirmOverlay');
  $('confirmIcon').innerHTML = icon;
  $('confirmTitle').textContent = title;
  $('confirmText').textContent = text;
  $('confirmYesBtn').innerHTML = yesLabel;
  $('confirmNoBtn').innerHTML = noLabel;

  const close = () => overlay.classList.remove('open');

  _vib('notificationWarning'); // тактильный отклик — диалог подтверждения (обычно деструктивное действие)
  $('confirmYesBtn').onclick = () => { _vib('impactMedium'); close(); onYes(); };
  $('confirmNoBtn').onclick = () => { _vib('tick'); close(); };
  overlay.onclick = e => { if(e.target===overlay){_vib('tick');close();} };

  overlay.classList.add('open');
}

// ══════════════════════════════════════════════════════
// ── SETTINGS MODAL ──
// ══════════════════════════════════════════════════════
function openSettingsModal(){
  let modal = document.getElementById('settingsModal');
  if(!modal){
    modal = document.createElement('div');
    modal.id = 'settingsModal';
    modal.className = 'modal-overlay';
    // Строим sheet с новым дизайном
    modal.innerHTML = `
      <div class="settings-sheet">
        <div class="modal-handle" style="width:36px;height:4px;background:rgba(255,255,255,.15);border-radius:2px;margin:14px auto 0"></div>

        <!-- Header -->
        <div class="settings-hdr">
          <div class="settings-hdr-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M9.671 4.136a2.34 2.34 0 0 1 4.659 0 2.34 2.34 0 0 0 3.319 1.915 2.34 2.34 0 0 1 2.33 4.033 2.34 2.34 0 0 0 0 3.831 2.34 2.34 0 0 1-2.33 4.033 2.34 2.34 0 0 0-3.319 1.915 2.34 2.34 0 0 1-4.659 0 2.34 2.34 0 0 0-3.32-1.915 2.34 2.34 0 0 1-2.33-4.033 2.34 2.34 0 0 0 0-3.831A2.34 2.34 0 0 1 6.35 6.051a2.34 2.34 0 0 0 3.319-1.915" /> <circle cx="12" cy="12" r="3" /> </svg></div>
          <span class="settings-hdr-title">Настройки</span>
        </div>

        <!-- Мой ID -->
        <div class="settings-sec">
          <div class="settings-sec-label">Мой ID</div>
          <div class="settings-card">
            <div class="settings-id-row">
              <span class="settings-id-val" id="settingsMyId">${MY_ID}</span>
              <div class="settings-copy-pill" id="settingsCopyPill"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <rect width="8" height="4" x="8" y="2" rx="1" ry="1" /> <path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2" /> </svg> Копировать</div>
            </div>
          </div>
        </div>

        <!-- Акцентный цвет -->
        <div class="settings-sec">
          <div class="settings-sec-label">Акцентный цвет</div>
          <div class="settings-card">
            <div class="settings-accent-grid" id="settingsAccentGrid"></div>
          </div>
        </div>

        <!-- Конфиденциальность -->
        <div class="settings-sec">
          <div class="settings-sec-label">Конфиденциальность</div>
          <div class="settings-card">
            <div class="settings-tog-row" id="lastSeenRow">
              <span class="settings-tog-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <circle cx="12" cy="12" r="10" /> <path d="M12 6v6l4 2" /> </svg></span>
              <div class="settings-tog-info">
                <div class="settings-tog-title">Время захода</div>
                <div class="settings-tog-desc">Показывать другим, когда вы были в сети</div>
              </div>
              <div class="settings-tog-sw" id="lastSeenSw"><div class="settings-tog-knob"></div></div>
            </div>
            <div style="height:1px;background:rgba(255,255,255,.06);margin:0 2px"></div>
            <div class="settings-tog-row" id="energySaveRow">
              <span class="settings-tog-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M4 14a1 1 0 0 1-.78-1.63l9.9-10.2a.5.5 0 0 1 .86.46l-1.92 6.02A1 1 0 0 0 13 10h7a1 1 0 0 1 .78 1.63l-9.9 10.2a.5.5 0 0 1-.86-.46l1.92-6.02A1 1 0 0 0 11 14z" /> </svg></span>
              <div class="settings-tog-info">
                <div class="settings-tog-title">Энергосбережение</div>
                <div class="settings-tog-desc">Отключает тяжёлые анимации — экономит батарею</div>
              </div>
              <div class="settings-tog-sw" id="energySaveSw"><div class="settings-tog-knob"></div></div>
            </div>
            <div style="height:1px;background:rgba(255,255,255,.06);margin:0 2px"></div>
            <div class="settings-tog-row" id="hapticsRow">
              <span class="settings-tog-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="m2 8 2 2-2 2 2 2-2 2" /> <path d="m22 8-2 2 2 2-2 2 2 2" /> <rect width="8" height="14" x="8" y="5" rx="1" /> </svg></span>
              <div class="settings-tog-info">
                <div class="settings-tog-title">Вибрация</div>
                <div class="settings-tog-desc">Тактильный отклик на действия: запись, отправка, кнопки</div>
              </div>
              <div class="settings-tog-sw" id="hapticsSw"><div class="settings-tog-knob"></div></div>
            </div>
            <div style="height:1px;background:rgba(255,255,255,.06);margin:0 2px"></div>
            <div class="settings-tog-row" id="bioLockRow" style="display:none">
              <span class="settings-tog-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M12 10a2 2 0 0 0-2 2c0 1.02-.1 2.51-.26 4" /> <path d="M14 13.12c0 2.38 0 6.38-1 8.88" /> <path d="M17.29 21.02c.12-.6.43-2.3.5-3.02" /> <path d="M2 12a10 10 0 0 1 18-6" /> <path d="M2 16h.01" /> <path d="M21.8 16c.2-2 .131-5.354 0-6" /> <path d="M5 19.5C5.5 18 6 15 6 12a6 6 0 0 1 .34-2" /> <path d="M8.65 22c.21-.66.45-1.32.57-2" /> <path d="M9 6.8a6 6 0 0 1 9 5.2v2" /> </svg></span>
              <div class="settings-tog-info">
                <div class="settings-tog-title">Блокировка по биометрии</div>
                <div class="settings-tog-desc" id="bioLockDesc">Touch ID / Face ID при открытии приложения</div>
              </div>
              <div class="settings-tog-sw" id="bioLockSw"><div class="settings-tog-knob"></div></div>
            </div>
          </div>
        </div>

        <!-- Опасная зона -->
        <div class="settings-sec">
          <div class="settings-sec-label">Опасная зона</div>
          <div class="settings-danger-card">
            <div class="settings-danger-row" id="settingsDangerRow">
              <span class="settings-danger-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg></span>
              <div class="settings-danger-info">
                <div class="settings-danger-title">Стереть все данные</div>
                <div class="settings-danger-desc">Удаление необратимо — восстановление невозможно</div>
              </div>
              <span class="settings-danger-chevron" id="settingsDangerChevron">›</span>
            </div>
            <div id="settingsDangerConfirm" style="display:none" class="settings-danger-confirm">
              <div class="settings-danger-confirm-text">Все сообщения, контакты и ключи шифрования будут удалены без возможности восстановления. Вы уверены?</div>
              <div class="settings-danger-confirm-btns">
                <button class="settings-dcbtn-cancel" id="settingsDcCancel">Отмена</button>
                <button class="settings-dcbtn-yes" id="settingsDcYes">Удалить</button>
              </div>
            </div>
          </div>
        </div>

        <!-- Версия -->
        <div class="settings-version">
          <span>v1.0.5.0</span>
          <div class="settings-version-dot"></div>
          <span>K-Chat E2EE</span>
        </div>

        <!-- Кнопка закрыть -->
        <div style="padding:4px 14px 0">
          <button class="btn btn-ghost" onclick="closeSettingsModal()" style="margin:0;border-radius:12px;font-size:14px;padding:12px">Закрыть</button>
        </div>
      </div>
    `;
    modal.addEventListener('click', e => { if(e.target===modal) closeSettingsModal(); });
    document.body.appendChild(modal);
  }

  // Обновляем ID
  const idEl = document.getElementById('settingsMyId');
  if(idEl) idEl.textContent = MY_ID;

  // Копирование ID
  const copyPill = document.getElementById('settingsCopyPill');
  if(copyPill){
    copyPill.onclick = () => {
      copyToClipboard(MY_ID).then(()=>{
        copyPill.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M20 6 9 17l-5-5\" /> </svg> Скопировано';
        copyPill.style.background = 'var(--g-20)';
        setTimeout(()=>{copyPill.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <rect width=\"8\" height=\"4\" x=\"8\" y=\"2\" rx=\"1\" ry=\"1\" /> <path d=\"M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2\" /> </svg> Копировать';copyPill.style.background='';},1500);
        toast('ID скопирован ✓');
      });
    };
  }

  // Рендерим акцентные свотчи
  const grid = document.getElementById('settingsAccentGrid');
  if(grid){
    const currentTheme = lsGet('bc_theme','green');
    grid.innerHTML = '';
    THEMES.forEach(t => {
      const opt = document.createElement('div');
      opt.className = 'settings-accent-opt' + (t.id===currentTheme?' active':'');
      const sw = document.createElement('div');
      sw.className = 'settings-accent-sw' + (t.id===currentTheme?' active':'');
      sw.style.background = t.grad;
      if(t.id===currentTheme){
        const chk = document.createElement('div');
        chk.className = 'settings-accent-sw-check';
        chk.innerHTML = '<svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M2.5 7L5.5 10L11.5 4" stroke="white" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>';
        sw.appendChild(chk);
      }
      const lbl = document.createElement('span');
      lbl.className = 'settings-accent-lbl';
      lbl.textContent = t.label;
      opt.appendChild(sw);
      opt.appendChild(lbl);
      opt.onclick = () => {
        applyTheme(t.id);
        // Перерисовываем сетку
        const g2 = document.getElementById('settingsAccentGrid');
        if(g2){
          const cur = t.id;
          g2.querySelectorAll('.settings-accent-opt').forEach((el,i)=>{
            const tid = THEMES[i]?.id;
            el.classList.toggle('active', tid===cur);
            const s2 = el.querySelector('.settings-accent-sw');
            s2.classList.toggle('active', tid===cur);
            let chk2 = s2.querySelector('.settings-accent-sw-check');
            if(tid===cur && !chk2){
              chk2 = document.createElement('div');
              chk2.className = 'settings-accent-sw-check';
              chk2.innerHTML = '<svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M2.5 7L5.5 10L11.5 4" stroke="white" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>';
              s2.appendChild(chk2);
            } else if(tid!==cur && chk2){
              chk2.remove();
            }
            const lbl2 = el.querySelector('.settings-accent-lbl');
            if(lbl2) lbl2.className = 'settings-accent-lbl' + (tid===cur?' active':'');
          });
        }
        toast('Тема: '+t.label);
      };
      grid.appendChild(opt);
    });
  }

  // Toggle "Время захода" — без input[type=checkbox], JS-управляемый
  const lastSeenEnabled = lsGet('bc_last_seen_enabled', false);
  const sw = document.getElementById('lastSeenSw');
  const row = document.getElementById('lastSeenRow');
  if(sw) sw.classList.toggle('on', lastSeenEnabled);
  if(row){
    row.onclick = () => {
      const sw2 = document.getElementById('lastSeenSw');
      if(!sw2) return;
      const nowOn = sw2.classList.toggle('on');
      lsSet('bc_last_seen_enabled', nowOn);
      _vib('tick');
      if(!nowOn) broadcastLastSeenOff().catch(()=>{});
      toast(nowOn ? 'Время захода включено' : 'Время захода скрыто');
    };
  }

  // Toggle "⚡ Энергосбережение"
  const esSaved = lsGet('bc_energy_save', false);
  const esSw  = document.getElementById('energySaveSw');
  const esRow = document.getElementById('energySaveRow');
  if(esSw) esSw.classList.toggle('on', esSaved);
  if(esRow){
    esRow.onclick = () => {
      const esSw2 = document.getElementById('energySaveSw');
      if(!esSw2) return;
      const nowOn = esSw2.classList.toggle('on');
      lsSet('bc_energy_save', nowOn);
      applyEnergySave(nowOn);
      toast(nowOn ? 'Энергосбережение включено' : 'Анимации восстановлены');
    };
  }

  // Toggle "📳 Вибрация"
  const hapSaved = lsGet('bc_haptics_enabled', true);
  const hapSw  = document.getElementById('hapticsSw');
  const hapRow = document.getElementById('hapticsRow');
  if(hapSw) hapSw.classList.toggle('on', hapSaved);
  if(hapRow){
    hapRow.onclick = () => {
      const hapSw2 = document.getElementById('hapticsSw');
      if(!hapSw2) return;
      const nowOn = hapSw2.classList.toggle('on');
      lsSet('bc_haptics_enabled', nowOn);
      // Сразу даём почувствовать включение/выключение —
      // если включаем, временно форсируем вибрацию для демонстрации
      if(nowOn){_vib('impactMedium');}
      else if(navigator.vibrate){try{navigator.vibrate(20);}catch(e){}}
      toast(nowOn ? 'Вибрация включена' : 'Вибрация выключена');
    };
  }

  // ── Тоггл биометрии — инициализация ──
  (function(){
    var bioRow  = document.getElementById('bioLockRow');
    var bioSw   = document.getElementById('bioLockSw');
    var bioDesc = document.getElementById('bioLockDesc');
    if(!bioRow || !bioSw) return;

    // Проверяем наличие Median API
    if(typeof median === 'undefined' || !median || !median.auth || !median.auth.status){
      bioRow.style.display = 'none'; // не в Median — прячем
      return;
    }

    // Проверяем доступность биометрии через callback (единственный надёжный способ)
    median.auth.status({callbackFunction: function(data){
      if(!data || !data.hasTouchId){
        bioRow.style.display = 'none';
        return;
      }
      // Биометрия есть — показываем строку
      bioRow.style.display = 'flex';
      var enabled = lsGet('bc_bio_lock', false);
      bioSw.classList.toggle('on', enabled);
      if(bioDesc) bioDesc.textContent = enabled
        ? 'Включена — запрашивается при открытии приложения'
        : 'Touch ID / Face ID при открытии приложения';

      bioRow.onclick = function(){
        var bioSw2 = document.getElementById('bioLockSw');
        if(!bioSw2) return;
        var isOn = bioSw2.classList.contains('on');
        if(!isOn){
          // Включаем: сначала удалим старый секрет (если был), затем сохраним новый
          median.auth.delete({callbackFunction: function(){
            median.auth.save({secret:'kchat_bio_unlock', callbackFunction: function(res){
              if(res && res.success){
                bioSw2.classList.add('on');
                lsSet('bc_bio_lock', true);
                var d2 = document.getElementById('bioLockDesc');
                if(d2) d2.textContent = 'Включена — запрашивается при открытии приложения';
                toast('Биометрия включена');
              } else {
                // duplicateItem — секрет уже существует, считаем успехом
                var errCode = res && res.error;
                if(errCode === 'duplicateItem'){
                  bioSw2.classList.add('on');
                  lsSet('bc_bio_lock', true);
                  var d3 = document.getElementById('bioLockDesc');
                  if(d3) d3.textContent = 'Включена — запрашивается при открытии приложения';
                  toast('Биометрия включена');
                } else {
                  toast('Ошибка сохранения биометрии: ' + (errCode||'?'), 'err');
                }
              }
            }});
          }});
        } else {
          // Выключаем
          median.auth.delete({callbackFunction: function(){
            bioSw2.classList.remove('on');
            lsSet('bc_bio_lock', false);
            var d2 = document.getElementById('bioLockDesc');
            if(d2) d2.textContent = 'Touch ID / Face ID при открытии приложения';
            toast('Биометрия отключена');
          }});
        }
      };
    }});
  })();

  // Danger zone — открываем отдельный sheet-диалог
  // Шаг 1: открываем inline аккордеон
  let _dangerOpen = false;
  const dangerRow = document.getElementById('settingsDangerRow');
  const dangerConfirm = document.getElementById('settingsDangerConfirm');
  const dangerChevron = document.getElementById('settingsDangerChevron');
  const dcCancel = document.getElementById('settingsDcCancel');
  const dcYes = document.getElementById('settingsDcYes');

  function _toggleDanger(){
    _dangerOpen = !_dangerOpen;
    if(dangerConfirm) dangerConfirm.style.display = _dangerOpen ? 'block' : 'none';
    if(dangerChevron) dangerChevron.style.transform = _dangerOpen ? 'rotate(90deg)' : 'rotate(0deg)';
  }
  if(dangerRow) dangerRow.onclick = _toggleDanger;
  if(dcCancel) dcCancel.onclick = e => { e.stopPropagation(); _toggleDanger(); };
  // Шаг 2: «Удалить» → открываем sheet-диалог с вводом подтверждения
  if(dcYes) dcYes.onclick = e => {
    e.stopPropagation();
    _toggleDanger(); // закрываем аккордеон
    openDangerConfirmDialog(); // открываем финальный sheet
  };

  modal.classList.add('open');
}

function openDangerConfirmDialog(){
  // Создаём диалог если нет
  let dlg = document.getElementById('dangerConfirmDialog');
  if(!dlg){
    dlg = document.createElement('div');
    dlg.id = 'dangerConfirmDialog';
    dlg.className = 'danger-dialog-overlay';
    dlg.innerHTML = `
      <div class="danger-dialog-sheet" id="dangerDialogSheet">
        <div class="danger-dialog-handle"></div>

        <div class="danger-dialog-header">
          <div class="danger-dialog-icon-wrap"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg></div>
          <div class="danger-dialog-header-text">
            <div class="danger-dialog-title">Стереть все данные</div>
            <div class="danger-dialog-subtitle">НЕОБРАТИМОЕ ДЕЙСТВИЕ</div>
          </div>
        </div>

        <div class="danger-dialog-body">
          <div class="danger-dialog-warning">
            <div class="danger-dialog-warning-row">
              <span class="danger-dialog-warning-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M2.992 16.342a2 2 0 0 1 .094 1.167l-1.065 3.29a1 1 0 0 0 1.236 1.168l3.413-.998a2 2 0 0 1 1.099.092 10 10 0 1 0-4.777-4.719" /> </svg></span>
              <span class="danger-dialog-warning-text">Все чаты и история сообщений будут удалены</span>
            </div>
            <div class="danger-dialog-warning-row">
              <span class="danger-dialog-warning-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="m15.5 7.5 2.3 2.3a1 1 0 0 0 1.4 0l2.1-2.1a1 1 0 0 0 0-1.4L19 4" /> <path d="m21 2-9.6 9.6" /> <circle cx="7.5" cy="15.5" r="5.5" /> </svg></span>
              <span class="danger-dialog-warning-text">Ключи шифрования и пароли будут удалены</span>
            </div>
            <div class="danger-dialog-warning-row">
              <span class="danger-dialog-warning-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <circle cx="12" cy="8" r="5" /> <path d="M20 21a8 8 0 0 0-16 0" /> </svg></span>
              <span class="danger-dialog-warning-text">Все контакты будут удалены</span>
            </div>
            <div class="danger-dialog-warning-row">
              <span class="danger-dialog-warning-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3" /> <path d="M12 9v4" /> <path d="M12 17h.01" /> </svg></span>
              <span class="danger-dialog-warning-text">Восстановление невозможно — данные удалятся навсегда</span>
            </div>
          </div>

          <div class="danger-dialog-input-wrap">
            <div class="danger-dialog-input-label">Введите «УДАЛИТЬ» для подтверждения</div>
            <input class="danger-dialog-input" id="dangerConfirmInput" type="text"
              placeholder="Введите УДАЛИТЬ" autocomplete="off" autocorrect="off"
              spellcheck="false" autocapitalize="off">
          </div>
        </div>

        <div class="danger-dialog-btns">
          <button class="danger-dialog-btn-yes" id="dangerDialogYes" disabled><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg> Удалить все данные</button>
          <button class="danger-dialog-btn-no" id="dangerDialogNo">Отмена</button>
        </div>
      </div>
    `;
    document.body.appendChild(dlg);

    // Закрыть по клику на фон
    dlg.addEventListener('click', e => { if(e.target === dlg) _closeDangerDialog(); });

    // Проверяем ввод
    const input = document.getElementById('dangerConfirmInput');
    const yesBtn = document.getElementById('dangerDialogYes');
    input.addEventListener('input', () => {
      const ok = input.value.trim().toUpperCase() === 'УДАЛИТЬ';
      yesBtn.disabled = !ok;
      yesBtn.style.background = ok ? 'rgba(255,79,106,.28)' : '';
      yesBtn.style.borderColor = ok ? 'rgba(255,79,106,.6)' : '';
    });

    // Отмена
    document.getElementById('dangerDialogNo').onclick = () => _closeDangerDialog();

    // Подтверждение — только если введено правильное слово
    yesBtn.onclick = () => {
      const input2 = document.getElementById('dangerConfirmInput');
      if(input2.value.trim().toUpperCase() !== 'УДАЛИТЬ') return;
      _chatsCache=null;
      localStorage.clear();
      indexedDB.deleteDatabase(DB_NAME);
      setTimeout(() => location.reload(), 200);
    };
  }

  // Сбрасываем состояние при каждом открытии
  const input = document.getElementById('dangerConfirmInput');
  const yesBtn = document.getElementById('dangerDialogYes');
  if(input){ input.value = ''; }
  if(yesBtn){ yesBtn.disabled = true; yesBtn.style.background = ''; yesBtn.style.borderColor = ''; }

  dlg.classList.add('open');
  setTimeout(() => { const inp = document.getElementById('dangerConfirmInput'); if(inp) inp.focus(); }, 380);
}

function _closeDangerDialog(){
  const dlg = document.getElementById('dangerConfirmDialog');
  if(dlg) dlg.classList.remove('open');
}
window._closeDangerDialog = _closeDangerDialog;

function closeSettingsModal(){
  const modal = document.getElementById('settingsModal');
  if(modal) modal.classList.remove('open');
}



async function exportAllData(){
  try{
    // Собираем все данные
    const data={
      version:2,
      exportedAt:new Date().toISOString(),
      myId:MY_ID,
      contacts:loadContacts(),
      chats:await loadChats(),
      messages:{},
      keys:{}
    };
    // Сообщения из IndexedDB
    const chats=await loadChats();
    for(const c of chats){
      const msgs=await loadMsgs(c.peerId);
      if(msgs.length)data.messages[c.peerId]=msgs;
    }
    // Ключи шифрования (зашифрованные)
    for(const k of Object.keys(localStorage)){
      if(k.startsWith('bc_secret_')||k.startsWith('bc_pwd_'))
        data.keys[k]=localStorage.getItem(k);
    }
    const blob=new Blob([JSON.stringify(data,null,2)],{type:'application/json'});
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a');
    a.href=url;a.download=`kchat-backup-${Date.now()}.json`;
    document.body.appendChild(a);a.click();document.body.removeChild(a);
    URL.revokeObjectURL(url);
    toast('Экспорт завершён ✓');
  }catch(e){toast('Ошибка экспорта','err');console.error(e);}
}

async function importAllData(e){
  const file=e.target.files[0];if(!file)return;
  e.target.value='';
  try{
    const text=await file.text();
    const data=JSON.parse(text);
    if(!data.version||!data.myId){toast('Неверный формат файла','err');return;}
    if(!confirm(`Импортировать данные от ${data.exportedAt}?\n\nСообщений: ${Object.values(data.messages||{}).reduce((s,a)=>s+a.length,0)}\n\nСуществующие данные будут объединены.`))return;
    
    // Импортируем контакты (объединяем)
    if(data.contacts){
      const existing=loadContacts();
      const merged=[...existing];
      for(const c of data.contacts){
        if(!merged.find(x=>x.id===c.id))merged.push(c);
      }
      saveContacts(merged);
    }
    
    // Импортируем чаты (объединяем)
    if(data.chats){
      const existing=await loadChats();
      const merged=[...existing];
      for(const c of data.chats){
        if(!merged.find(x=>x.peerId===c.peerId))merged.push(c);
      }
      await saveChats(merged);
    }
    
    // Импортируем сообщения (объединяем, старые не затираем)
    if(data.messages){
      for(const [pid,msgs] of Object.entries(data.messages)){
        await withChatLock(pid,async()=>{
          const existing=await loadMsgs(pid);
          const existingIds=new Set(existing.map(m=>m.id));
          const newMsgs=msgs.filter(m=>!existingIds.has(m.id));
          if(newMsgs.length){
            const merged=[...existing,...newMsgs].sort((a,b)=>new Date(a.time)-new Date(b.time));
            await saveMsgs(pid,merged);
          }
        });
      }
    }
    
    // Импортируем ключи шифрования (только если не существуют)
    if(data.keys){
      for(const [k,v] of Object.entries(data.keys)){
        if(!localStorage.getItem(k))localStorage.setItem(k,v);
      }
    }
    
    renderChatList();
    toast('Импорт завершён ✓');
    closeSettingsModal();
  }catch(e){toast('Ошибка импорта: '+e.message,'err');console.error(e);}
}

const hl=(str,q)=>{if(!q)return str;const re=new RegExp(`(${q.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')})`,'gi');return str.replace(re,'<span class="search-highlight">$1</span>');};

// ── Закрепить / Открепить чат ──
async function pinChat(pid){
  const chats=await loadChats();
  const c=chats.find(x=>x.peerId===pid);
  if(c){
    c.pinned=true;
    // pinOrder — время закрепления. Чем больше → тем выше в списке (как в Telegram).
    c.pinOrder=Date.now();
    await saveChats(chats);
  }
  renderChatList();
  toast('Чат закреплён');
}
async function unpinChat(pid){
  const chats=await loadChats();
  const c=chats.find(x=>x.peerId===pid);
  if(c){c.pinned=false;c.pinOrder=0;await saveChats(chats);}
  renderChatList();
  toast('Чат откреплён');
}

// ── Контекстное меню чата на главном экране ──
async function showChatCtx(pid,tx,ty){
  const chats=await loadChats();
  const chat=chats.find(c=>c.peerId===pid)||{};
  const isPinned=!!chat.pinned&&pid!==MY_ID;
  const isBlocked=isContactBlocked(pid);
  closeAll();mkBack();
  const menu=document.createElement('div');menu.className='ctx-menu';
  const add=(ic,lb,fn,danger=false)=>{
    const el=document.createElement('div');
    el.className='ctx-item'+(danger?' danger':'');
    el.innerHTML='<span>'+ic+'</span><span>'+lb+'</span>';
    el.addEventListener('click',e=>{e.stopPropagation();fn();});
    menu.appendChild(el);
  };
  const divider=()=>{const d=document.createElement('div');d.className='ctx-divider';menu.appendChild(d);};
  // Закрепить / Открепить
  if(pid!==MY_ID){
    if(isPinned){
      add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 17v5\" /> <path d=\"M15 9.34V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H7.89\" /> <path d=\"m2 2 20 20\" /> <path d=\"M9 9v1.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h11\" /> </svg>','Открепить',()=>{closeAll();unpinChat(pid);});
    }else{
      add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 17v5\" /> <path d=\"M9 10.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H8a2 2 0 0 0 0 4 1 1 0 0 1 1 1z\" /> </svg>','Закрепить',()=>{closeAll();pinChat(pid);});
    }
  }
  // Пометить как непрочитанное
  // Пометить: если уже помечено — снять пометку, иначе — поставить
  const isMarked=!!chat.marked;
  add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M17 3a2 2 0 0 1 2 2v15a1 1 0 0 1-1.496.868l-4.512-2.578a2 2 0 0 0-1.984 0l-4.512 2.578A1 1 0 0 1 5 20V5a2 2 0 0 1 2-2z\" /> </svg>',isMarked?'Снять пометку':'Пометить',async()=>{
    closeAll();
    if(isMarked){
      await updateChat(pid,{marked:false});
      toast('Пометка снята');
    }else{
      await updateChat(pid,{marked:true});
      toast('Помечено');
    }
    renderChatList();
  });
  // Заблокировать / Разблокировать
  if(pid!==MY_ID){
    if(isBlocked){
      add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"m15 9-6 6\" /> <path d=\"m9 9 6 6\" /> </svg>','Разблокировать',()=>{
        closeAll();
        showConfirm({
          icon:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"m15 9-6 6\" /> <path d=\"m9 9 6 6\" /> </svg>',
          title:'Разблокировать контакт?',
          text:'Контакт будет разблокирован. Вы снова сможете получать от него сообщения и общаться.',
          yesLabel:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"m15 9-6 6\" /> <path d=\"m9 9 6 6\" /> </svg> Разблокировать',
          noLabel:'Отмена',
          onYes:()=>{
            setContactBlocked(pid,false);
            if(activePid===pid)updateBlockUI(pid);
            _sendBlockSignal(pid,false).catch(()=>{});
            renderChatList();
            toast('Контакт разблокирован');
          }
        });
      });
    }else{
      add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M4.929 4.929 19.07 19.071\" /> </svg>','Заблокировать',()=>{
        closeAll();
        showConfirm({
          icon:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M4.929 4.929 19.07 19.071\" /> </svg>',
          title:'Заблокировать контакт?',
          text:'Контакт будет заблокирован. Новые сообщения от него перестанут поступать. Вы сможете разблокировать его в любой момент.',
          yesLabel:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M4.929 4.929 19.07 19.071\" /> </svg> Заблокировать',
          noLabel:'Отмена',
          onYes:()=>{
            setContactBlocked(pid,true);
            if(activePid===pid)updateBlockUI(pid);
            _sendBlockSignal(pid,true).catch(()=>{});
            renderChatList();
            toast('Контакт заблокирован');
          }
        });
      });
    }
  }
  // Удалить
  divider();
  add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M10 11v6\" /> <path d=\"M14 11v6\" /> <path d=\"M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6\" /> <path d=\"M3 6h18\" /> <path d=\"M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2\" /> </svg>','Удалить',()=>{
    closeAll();
    if(pid===MY_ID){toast('Чат "Избранное" нельзя удалить','warn');return;}
    showConfirm({
      icon:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M10 11v6\" /> <path d=\"M14 11v6\" /> <path d=\"M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6\" /> <path d=\"M3 6h18\" /> <path d=\"M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2\" /> </svg>',
      title:'Удалить чат?',
      text:'История сообщений и ключ шифрования будут удалены с этого устройства. Собеседник не будет уведомлён.',
      yesLabel:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg> Удалить чат',
      noLabel:'Отмена',
      onYes:async()=>{
        if(activePid===pid){activePid='';stopPeerActivity();cancelEdit();cancelReply();resetMsgSearch();exitSelectionMode();closeAll();}
        try{
          const allMsgs=await loadMsgs(pid);
          for(const m of allMsgs){
            if(m.media?.blobId)deleteBlob(m.media.blobId);
            if(m.voice?.blobId)deleteBlob(m.voice.blobId);
            if(m.file?.blobId)deleteBlob(m.file.blobId);
          }
        }catch(e){}
        await clearMsgs(pid);
        await deleteChatData(pid);
        delete keyCache[pid];
        localStorage.removeItem('bc_pwd_'+pid);
        localStorage.removeItem('bc_secret_'+pid);
        sessionStorage.removeItem('bc_pwd_'+pid);
        if(activePid===''&&document.getElementById('scr-chat')?.classList.contains('active'))goHome();
        toast('Чат удалён');
      }
    });
  },true);
  document.body.appendChild(menu);_ctx=menu;
  requestAnimationFrame(()=>{
    const mw=menu.offsetWidth||190,mh=menu.offsetHeight||60;
    const vw=window.innerWidth,vh=window.innerHeight;
    let l=tx,t=ty;
    if(l+mw>vw-8)l=vw-mw-8;
    if(t+mh>vh-8)t=vh-mh-8;
    if(l<8)l=8;if(t<8)t=8;
    menu.style.left=l+'px';menu.style.top=t+'px';
  });
}

async function renderChatList(){
  const list=$('chatList'),empty=$('emptyChatsPlaceholder');
  const q=($('homeSearch')?.value||'').toLowerCase().trim();
  let chats=await loadChats();
  // Сортировка: Избранное → Закреплённые (по pinOrder убыв.) → Остальные (по lastMsgTime)
  // pinOrder — timestamp закрепления. Последний закреплённый → вверху (как в Telegram).
  // Новые сообщения НЕ меняют порядок закреплённых чатов.
  chats=chats.sort((a,b)=>{
    if(a.peerId===MY_ID)return -1;
    if(b.peerId===MY_ID)return 1;
    const aPinned=!!a.pinned&&a.peerId!==MY_ID;
    const bPinned=!!b.pinned&&b.peerId!==MY_ID;
    if(aPinned&&!bPinned)return -1;
    if(!aPinned&&bPinned)return 1;
    if(aPinned&&bPinned){
      // Оба закреплены — сортируем по pinOrder убыванию:
      // чем позже закреплён → тем выше (как в Telegram)
      return (b.pinOrder||0)-(a.pinOrder||0);
    }
    // Оба не закреплены — по времени последнего сообщения
    return (b.lastMsgTime||0)-(a.lastMsgTime||0);
  });
  if(q)chats=chats.filter(c=>(c.peerName||'').toLowerCase().includes(q)||(c.peerId||'').toLowerCase().includes(q)||(c.lastMsg||'').toLowerCase().includes(q));
  if(currentTab==='groups')chats=chats.filter(c=>!!c.isGroup);
  else chats=chats.filter(c=>!c.isGroup);
  if(!chats.length){list.innerHTML='';empty.style.display='block';empty.textContent=currentTab==='groups'?'Нет групп':'Нет чатов';return;}
  empty.style.display='none';
  list.innerHTML='';
  const frag=document.createDocumentFragment();
  chats.forEach(c=>{
    const on=!!online[c.peerId];
    const isPinned=!!c.pinned&&c.peerId!==MY_ID;
    const isBlocked=isContactBlocked(c.peerId);
    const item=document.createElement('div');
    item.className='chat-item'+(isPinned?' pinned-chat':'')+(isBlocked?' blocked-chat':'');
    const nameHtml=q?hl(esc(c.peerName),q):esc(c.peerName);
    const lastHtml=q?hl(esc(c.lastMsg||''),q):esc(c.lastMsg||'Нет сообщений');
    const pinBadge=isPinned?'<span class="chat-pin-badge"><svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 17v5\" /> <path d=\"M9 10.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H8a2 2 0 0 0 0 4 1 1 0 0 1 1 1z\" /> </svg></span>':'';
    const blockBadge=isBlocked?'<span class="chat-pin-badge" style="opacity:.5"><svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M4.929 4.929 19.07 19.071\" /> </svg></span>':'';
    // Если есть реальные непрочитанные — показываем цифру
    // Если только пометка (marked) — показываем пустой кружок
    const badge=c.unread>0
      ?'<span class="chat-badge">'+c.unread+'</span>'
      :(c.marked?'<span class="chat-badge dot"></span>':'');
    item.innerHTML='<div class="chat-avatar">'+c.avatar+'<span class="presence-dot'+(on?' online':'')+'"></span></div>'
      +'<div class="chat-info"><div class="chat-name-row"><span class="chat-name">'+nameHtml+'</span>'+pinBadge+blockBadge+'</div>'
      +'<div class="chat-last-msg">'+lastHtml+'</div></div>'
      +'<div class="chat-meta"><span class="chat-time">'+fmtTime(c.lastMsgTime)+'</span>'+badge+'</div>';
    // Long-press / contextmenu → контекстное меню
    let _lpTimer=null,_lpFired=false;
    item.addEventListener('touchstart',e=>{
      _lpFired=false;
      _lpTimer=setTimeout(()=>{
        _lpFired=true;
        item.classList.add('ctx-pressed');
        setTimeout(()=>item.classList.remove('ctx-pressed'),300);
        const t=e.touches[0];
        showChatCtx(c.peerId,t.clientX,t.clientY);
      },500);
    },{passive:true});
    item.addEventListener('touchend',()=>clearTimeout(_lpTimer),{passive:true});
    item.addEventListener('touchmove',()=>clearTimeout(_lpTimer),{passive:true});
    item.addEventListener('contextmenu',e=>{
      e.preventDefault();
      showChatCtx(c.peerId,e.clientX,e.clientY);
    });
    item.addEventListener('click',()=>{if(!_lpFired)openChatById(c.peerId);});
    frag.appendChild(item);
  });
  list.appendChild(frag);
}

function renderContacts(){const q=($('contactsSearch')?.value||'').toLowerCase();let contacts=loadContacts();if(q)contacts=contacts.filter(c=>c.name.toLowerCase().includes(q)||c.id.toLowerCase().includes(q));const list=$('contactsList');if(!contacts.length){list.innerHTML=`<div class="empty-state"><div class="empty-state-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <circle cx="12" cy="8" r="5" /> <path d="M20 21a8 8 0 0 0-16 0" /> </svg></div><div class="empty-state-text">Нет контактов.<br>Нажмите «+ Добавить»</div></div>`;return;}list.innerHTML=contacts.map((c,i)=>{const on=!!online[c.id];return`<div class="contact-item"><div class="contact-avatar">${c.avatar||'👤'}<span class="presence-dot${on?' online':''}"></span></div><div class="contact-info" onclick="startChatWithContact('${esc(c.id)}')"><div class="contact-name">${esc(c.name)}</div><div class="contact-id">${esc(c.id)}</div></div><div class="contact-actions"><div class="icon-btn-sm" onclick="startChatWithContact('${esc(c.id)}')"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M2.992 16.342a2 2 0 0 1 .094 1.167l-1.065 3.29a1 1 0 0 0 1.236 1.168l3.413-.998a2 2 0 0 1 1.099.092 10 10 0 1 0-4.777-4.719" /> </svg></div><div class="contact-del-btn" onclick="delContact(${i},event)"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg></div></div></div>`;}).join('');}
window.delContact=(i,e)=>{
  e?.stopPropagation();
  const contacts=loadContacts();
  const c=contacts[i];
  if(!c)return;
  showConfirm({
    icon:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg>',
    title:'Удалить контакт?',
    text:`Контакт «${c.name}» будет удалён из списка. История переписки и ключи шифрования сохранятся.`,
    yesLabel:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg> Удалить',
    noLabel:'Отмена',
    onYes:()=>{
      const list=loadContacts();
      list.splice(i,1);
      saveContacts(list);
      renderContacts();
      toast('Контакт удалён');
    }
  });
};
window.startChatWithContact=async id=>{const ct=loadContacts().find(c=>c.id===id);await getOrCreateChat(id,ct?.name,ct?.avatar);await renderChatList();goHome();setTimeout(()=>openChatById(id),120);};

const AVTS=['👤','🤖','🦊','🐱','🐶','🦁','🐼','🦄','🔥','⭐','🌊','🍄','❤️','🌚'];
function initAvt(){$('avatarSelector').innerHTML=AVTS.map(e=>`<div class="icon-btn-sm" style="width:36px;height:36px;font-size:18px" onclick="selAvt('${e}',event)">${e}</div>`).join('');}
window.selAvt=(em,e)=>{$('newContactAvatar').value=em;document.querySelectorAll('#avatarSelector .icon-btn-sm').forEach(el=>{el.style.borderColor='var(--border)';el.style.background='rgba(0,255,136,.08)';});if(e?.currentTarget){e.currentTarget.style.borderColor='var(--border-hi)';e.currentTarget.style.background='rgba(0,255,136,.18)';}};
let _openChatAfterSave=false;
window.openAddContact=()=>{$('newContactId').value='';$('newContactName').value='';$('newContactAvatar').value='👤';document.querySelectorAll('#avatarSelector .icon-btn-sm').forEach(el=>{el.style.borderColor='var(--border)';el.style.background='rgba(0,255,136,.08)';});$('addContactModal').classList.add('open');setTimeout(()=>$('newContactName').focus(),300);};
window.closeAddContact=()=>{$('addContactModal').classList.remove('open');_openChatAfterSave=false;};
window.closeAddContactOverlay=e=>{if(e.target===$('addContactModal'))closeAddContact();};
window.saveContact=async()=>{const name=$('newContactName').value.trim(),id=$('newContactId').value.trim().toLowerCase(),avatar=$('newContactAvatar').value;if(!name){toast('Введите имя','err');return;}if(!id){toast('Введите ID','err');return;}if(id===MY_ID){toast('Нельзя добавить себя','err');return;}const contacts=loadContacts();if(contacts.find(c=>c.id===id)){toast('Такой ID уже есть','err');return;}contacts.push({name,id,added:Date.now(),avatar});saveContacts(contacts);renderContacts();toast(`«${name}» добавлен`);closeAddContact();if(_openChatAfterSave){_openChatAfterSave=false;await getOrCreateChat(id,name,avatar);await renderChatList();openChatById(id);}};

// ── SCREENS ──
let currentScreen='scr-home';
function goScreen(id){document.querySelectorAll('.screen').forEach(s=>{s.classList.remove('active');s.classList.add('hidden');});$(id).classList.remove('hidden');$(id).classList.add('active');currentScreen=id;}
window.goHome=()=>{closeFab();goScreen('scr-home');renderChatList();};
window.showContacts=()=>{renderContacts();goScreen('scr-contacts');};

// ══════════════════════════════════════════════════════════════════
// ── SHARE INTO APP (Median.co "Поделиться в приложении") ──
// Median вызывает window.median_share_to_app(data) когда пользователь
// выбирает K-Chat в системном меню "Поделиться" другого приложения.
// data = { url, subject, text } — пришедшая ссылка/текст.
// Мы показываем экран выбора чата (как в Telegram "Отправить"),
// пользователь выбирает один чат, жмёт ➤ — и текст/ссылка
// отправляется как обычное сообщение в выбранный чат.
//
// ВАЖНО: на Android Median иногда вызывает median_share_to_app()
// ПОВТОРНО — например при возврате webview из фона/после анимации
// системного шаринга — но уже с ПУСТЫМ data (или вовсе без аргумента).
// Если это происходит после того, как пользователь уже открыл экран
// выбора чата, повторный пустой вызов раньше ЗАТИРАЛ _sharePayloadText
// пустой строкой — и при нажатии "Отправить" в чат уходило пустое
// сообщение. Теперь пустой/повторный вызов без полезных данных
// полностью игнорируется и не трогает уже сохранённый payload.
// Дополнительно payload сохраняется в sessionStorage, чтобы пережить
// возможный ре-рендер/повторную инициализацию страницы.
// ══════════════════════════════════════════════════════════════════
let _shareSelectedPid=null;
let _sharePayloadText='';

function _extractShareText(data){
  data=data||{};
  // Поддерживаем все известные варианты полей, которые встречаются
  // у разных версий Median/устройств/Android share-intent мостов.
  const pick=(...keys)=>{
    for(const k of keys){
      const v=data[k];
      if(typeof v==='string'&&v.trim())return v.trim();
    }
    return '';
  };
  const url=pick('url','link','uri','sharedUrl');
  const subject=pick('subject','title','sharedTitle');
  const text=pick('text','body','message','sharedText','description');
  // Собираем итоговый текст для отправки: заголовок/текст + ссылка
  const parts=[];
  if(subject&&subject!==url)parts.push(subject);
  if(text&&text!==url&&text!==subject)parts.push(text);
  if(url&&!parts.includes(url))parts.push(url);
  return parts.join('\n');
}

window.median_share_to_app=function(data){
  try{
    const payload=_extractShareText(data);
    if(!payload){
      // Пустой повторный вызов (Median resume-quirk) — игнорируем,
      // НЕ трогаем уже сохранённый _sharePayloadText.
      console.warn('median_share_to_app: empty payload, ignored',data);
      return;
    }
    _sharePayloadText=payload;
    try{sessionStorage.setItem('_pendingSharePayload',payload);}catch(e){}
    openShareScreen();
  }catch(e){console.error('median_share_to_app error',e);}
};

// При старте страницы — если есть незавершённый shared-payload
// (например, страница была перезагружена пока был открыт экран
// выбора чата) — восстанавливаем его.
try{
  const _restored=sessionStorage.getItem('_pendingSharePayload');
  if(_restored)_sharePayloadText=_restored;
}catch(e){}

function openShareScreen(){
  _shareSelectedPid=null;
  // Превью того, чем делимся
  const pv=$('sharePreview');
  const isLink=/^https?:\/\//i.test(_sharePayloadText.split('\n').pop()||'');
  pv.innerHTML=`
    <div class="share-preview-icon">${isLink?'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71\" /> <path d=\"M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71\" /> </svg>':'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z\" /> <path d=\"M14 2v5a1 1 0 0 0 1 1h5\" /> <path d=\"M10 9H8\" /> <path d=\"M16 13H8\" /> <path d=\"M16 17H8\" /> </svg>'}</div>
    <div class="share-preview-body">
      <div class="share-preview-title">${isLink?'Ссылка':'Текст'} для отправки</div>
      <div class="share-preview-sub">${esc(_sharePayloadText)}</div>
    </div>`;
  renderShareTargets('');
  $('shareSendFab').classList.remove('visible');
  goScreen('scr-share');
}

function renderShareTargets(query){
  const q=(query||'').toLowerCase();
  let chats=[];
  try{chats=lsGet('bc_chats',[]);}catch(e){}
  let contacts=loadContacts();
  // Объединяем существующие чаты и контакты без чатов, плюс "Избранное"
  const seen=new Set();
  const targets=[];
  // Избранное (заметки самому себе) — всегда первым
  targets.push({peerId:MY_ID,peerName:'⭐ Избранное',avatar:'⭐'});
  seen.add(MY_ID);
  chats.forEach(c=>{if(!seen.has(c.peerId)){targets.push(c);seen.add(c.peerId);}});
  contacts.forEach(c=>{if(!seen.has(c.id)){targets.push({peerId:c.id,peerName:c.name,avatar:c.avatar||'👤'});seen.add(c.id);}});
  const filtered=q?targets.filter(t=>(t.peerName||'').toLowerCase().includes(q)||t.peerId.toLowerCase().includes(q)):targets;
  const list=$('shareTargetList');
  if(!filtered.length){
    list.innerHTML=`<div class="empty-state"><div class="empty-state-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <circle cx="12" cy="8" r="5" /> <path d="M20 21a8 8 0 0 0-16 0" /> </svg></div><div class="empty-state-text">Нет чатов для отправки.<br>Сначала добавьте контакт.</div></div>`;
    return;
  }
  list.innerHTML=filtered.map(t=>{
    const sel=t.peerId===_shareSelectedPid;
    return `<div class="share-target-item${sel?' selected':''}" data-pid="${esc(t.peerId)}" onclick="selectShareTarget('${esc(t.peerId)}')">
      <div class="contact-avatar">${t.avatar||'👤'}</div>
      <div class="contact-info" style="cursor:default">
        <div class="contact-name">${esc(t.peerName||t.peerId)}</div>
        <div class="contact-id">${esc(t.peerId)}</div>
      </div>
      <div class="share-target-check"></div>
    </div>`;
  }).join('');
}

window.selectShareTarget=function(pid){
  _shareSelectedPid=(_shareSelectedPid===pid)?null:pid;
  $('shareTargetList').querySelectorAll('.share-target-item').forEach(el=>{
    el.classList.toggle('selected',el.dataset.pid===_shareSelectedPid);
  });
  $('shareSendFab').classList.toggle('visible',!!_shareSelectedPid);
};

// Ждём подключения WebSocket (с таймаутом). На холодном старте
// приложения (например, когда оно запущено системой ради приёма
// "Поделиться") установление соединения занимает заметно больше,
// чем 150мс — раньше из-за этого sendSingleMsg сразу падал с
// "offline" и пользователь видел "Не удалось отправить", хотя
// на самом деле соединение просто ещё не успело подняться.
function waitForWS(timeoutMs=8000){
  return new Promise(resolve=>{
    if(wsUp){resolve(true);return;}
    ensureWS();
    const start=Date.now();
    const iv=setInterval(()=>{
      if(wsUp){clearInterval(iv);resolve(true);return;}
      if(Date.now()-start>timeoutMs){clearInterval(iv);resolve(false);}
    },100);
  });
}

window.confirmShareSend=async function(){
  if(!_shareSelectedPid)return;
  // Доп. защита: если payload вдруг пуст (например, был затёрт до этого
  // фикса или storage пуст) — не отправляем пустое сообщение,
  // показываем ошибку и закрываем экран шаринга.
  if(!_sharePayloadText||!_sharePayloadText.trim()){
    toast('Нет данных для отправки','err');
    closeShareScreen();
    return;
  }
  const pid=_shareSelectedPid;
  const textToSend=_sharePayloadText;
  const fab=$('shareSendFab');
  fab.style.pointerEvents='none';
  fab.style.opacity='.6';
  try{
    // Если отправляем не в "Избранное" — нужна связь с сервером.
    // На холодном старте (приложение только что открыто системой
    // через "Поделиться") WS может ещё не успеть подняться —
    // подождём, показывая пользователю статус подключения.
    if(pid!==MY_ID&&!wsUp){
      toast('Подключение...');
      const ok=await waitForWS(8000);
      if(!ok){
        toast('Нет связи с сервером. Сообщение сохранено, нажмите кнопку ещё раз','err');
        fab.style.pointerEvents='';
        fab.style.opacity='';
        return; // не закрываем экран шаринга — payload сохранён, можно повторить отправку
      }
    }
    const ct=loadContacts().find(c=>c.id===pid);
    await getOrCreateChat(pid,ct?.name,ct?.avatar);
    closeShareScreen();
    goHome();
    setTimeout(async()=>{
      await openChatById(pid);
      activePid=pid;
      try{
        await sendSingleMsg(textToSend);
      }catch(e){
        console.error('confirmShareSend send error',e);
        toast('Не удалось отправить','err');
      }
    },150);
  }finally{
    fab.style.pointerEvents='';
    fab.style.opacity='';
  }
};

window.closeShareScreen=function(){
  _shareSelectedPid=null;_sharePayloadText='';
  try{sessionStorage.removeItem('_pendingSharePayload');}catch(e){}
  goHome();
};

let activePid='',renderedCount=0;
window.openChatById=async function(pid){
  closeFab();await updateChat(pid,{unread:0,marked:false});resetMsgSearch();exitSelectionMode();goScreen('scr-chat');
  activePid=pid;stopPeerActivity();
  const chat=(await loadChats()).find(c=>c.peerId===pid)||{};
  $('peerName').textContent=chat.peerName||pid;
  setPeerStatus('offline','Не в сети');$('sendBtn').disabled=true;_startConnDots('Подключение');
  renderedCount=0;clearMediaPreview();
  // Инициализируем UI блокировки
  const blockBtn=$('blockContactBtn');
  if(blockBtn){blockBtn.style.display=(pid===MY_ID)?'none':'flex';}
  // Сбрасываем баннер к дефолту
  const banner=$('blockedBanner');
  if(banner)banner.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M4.929 4.929 19.07 19.071\" /> </svg> Контакт заблокирован — отправка сообщений недоступна';
  updateBlockUI(pid);
  // Если нас заблокировали — показываем баннер "вы заблокированы"
  if(lsGet(`bc_blocked_by_${pid}`,false)&&!isContactBlocked(pid)){
    setPeerStatus('blocked','Вы заблокированы');
    const inputBar=$('chatInputBar');
    if(inputBar)inputBar.classList.add('blocked-mode');
    if(banner){banner.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M4.929 4.929 19.07 19.071\" /> </svg> Вы заблокированы этим контактом';banner.classList.add('visible');}
  }
  await loadChatHistory(pid,true);
  initPinnedBar(pid);
  initAutoDelBtn(pid);
  ensureWS();warmKey(pid);
  if(wsUp){updateSendBtn();wsSend({type:'query-presence',target:pid});}
  const key=await getKey(pid);
  if(key){
    await drainPending(pid,key);
    // После drain пересчитываем renderedCount из реальной БД перед дозагрузкой
    if(activePid===pid){
      const msgsAfterDrain=await loadMsgs(pid);
      const domRows=$('messagesArea')?.querySelectorAll('.msg-row[data-msgid],.sys-msg[data-msgid]');
      // renderedCount = кол-во строк в DOM (не в БД!) — чтобы slice не захватил лишнее
      renderedCount=domRows?domRows.length:renderedCount;
      await loadChatHistory(pid,false);
    }
  }
  if(activePid===pid)requestAnimationFrame(()=>{const a=$('messagesArea');if(a)a.scrollTop=a.scrollHeight;});
  // Отправляем read-receipt собеседнику — у него появятся ✔✔ на его отправленных
  // Делаем это с небольшой задержкой чтобы чат успел отрисоваться
  if(pid !== MY_ID){
    // Flush pending acks — отправляем серверу ack-msg для всех сообщений этого чата
    // Сервер пошлёт msg-delivered отправителю → у него появятся ✔✔
    _flushPendingAcks(pid);
    // Также шлём chat-read зашифрованный envelope (для файлов и старых сообщений)
    setTimeout(()=>{ if(activePid===pid) _sendChatReadReceipt(pid).catch(()=>{}); }, 600);
  }
};

window.forgetPassword=()=>{
  if(!activePid)return;
  showConfirm({
    icon:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <circle cx="12" cy="16" r="1" /> <rect width="18" height="12" x="3" y="10" rx="2" /> <path d="M7 10V7a5 5 0 0 1 9.33-2.5" /> </svg>',
    title:'Удалить ключ доступа?',
    text:'Ключ шифрования и пароль для этого чата будут удалены. При следующем сообщении потребуется ввести пароль заново.',
    yesLabel:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <circle cx="12" cy="16" r="1" /> <rect width="18" height="12" x="3" y="10" rx="2" /> <path d="M7 10V7a5 5 0 0 1 9.33-2.5" /> </svg> Удалить ключ',
    noLabel:'Отмена',
    onYes:()=>{
      delete keyCache[activePid];
      localStorage.removeItem(`bc_pwd_${activePid}`);
      localStorage.removeItem(`bc_secret_${activePid}`);
      sessionStorage.removeItem(`bc_pwd_${activePid}`);
      toast('Ключ удалён');
    }
  });
};
function formatText(text){if(!text)return'';let html=esc(text);html=html.replace(/\*([^*]+)\*/g,'<b>$1</b>');html=html.replace(/_([^_]+)_/g,'<i>$1</i>');html=html.replace(/~([^~]+)~/g,'<s>$1</s>');html=html.replace(/\|\|([^|]+)\|\|/g,'<span class="spoiler">$1</span>');html=html.replace(/(https?:\/\/[^\s<]+)/g,'<a href="$1" target="_blank" rel="noopener noreferrer">$1</a>');return html;}

async function forwardToFavorites(msgId){const msgs=await loadMsgs(activePid);const m=msgs.find(m=>m.id===msgId);if(!m)return;const newMsg={id:uid(),text:m.text||'',type:'sent',time:new Date().toISOString(),reactions:{},edited:false,delivered:true,forwarded:true};if(m.media){newMsg.media=m.media;newMsg.caption=m.caption;}if(m.voice){newMsg.voice=m.voice;newMsg.voiceDuration=m.voiceDuration;newMsg.voiceListened=true;}if(m.file){newMsg.file=m.file;}await upsertMsg(MY_ID,newMsg);const lastPreview=m.file?`${m.file.name}`:m.voice?'Голосовое':m.media?'Фото':(m.text||'').slice(0,28);await updateChat(MY_ID,{lastMsg:lastPreview,lastMsgTime:Date.now()});if(activePid===MY_ID)appendOrReloadMsg(MY_ID,newMsg);toast('Переслано в Избранное');}

function compressImage(file,maxWidth=1024,maxHeight=1024,quality=0.7){return new Promise((resolve,reject)=>{const reader=new FileReader();reader.onload=e=>{const img=new Image();img.onload=()=>{let w=img.width,h=img.height;if(w>maxWidth||h>maxHeight){const r=Math.min(maxWidth/w,maxHeight/h);w=Math.round(w*r);h=Math.round(h*r);}const c=document.createElement('canvas');c.width=w;c.height=h;c.getContext('2d').drawImage(img,0,0,w,h);resolve({dataURL:c.toDataURL('image/jpeg',quality),type:'image/jpeg',name:file.name});};img.onerror=reject;img.src=e.target.result;};reader.onerror=reject;reader.readAsDataURL(file);});}

let pendingFiles=[];
function updateFilePreview(){const container=$('mediaPreview');if(!pendingFiles.length){$('mediaPreviewContainer').style.display='none';return;}$('mediaPreviewContainer').style.display='flex';const file=pendingFiles[0];if(file.isMedia&&file.type.startsWith('image/')&&file.data)container.innerHTML=`<img class="media-preview-img" src="${file.data}"><span class="media-preview-info"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <rect width="18" height="18" x="3" y="3" rx="2" ry="2" /> <circle cx="9" cy="9" r="2" /> <path d="m21 15-3.086-3.086a2 2 0 0 0-2.828 0L6 21" /> </svg> ${pendingFiles.length>1?pendingFiles.length+' фото':file.name}</span>`;else if(file.isVideo||file.type.startsWith('video/'))container.innerHTML=`<span style="class="media-preview-file-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="m16 13 5.223 3.482a.5.5 0 0 0 .777-.416V7.87a.5.5 0 0 0-.752-.432L16 10.5" /> <rect x="2" y="6" width="14" height="12" rx="2" /> </svg></span><span class="media-preview-info">${pendingFiles.length>1?pendingFiles.length+' видео':file.name} (${formatSize(file.size)})</span>`;else{const icon=getFileIcon(file.type);container.innerHTML=`<span style="font-size:36px">${icon}</span><span class="media-preview-info">${file.name} (${formatSize(file.size)})</span>`;}}
window.clearMediaPreview=()=>{pendingFiles=[];$('mediaPreviewContainer').style.display='none';$('mediaPreview').innerHTML='';};

let processingFiles=false,queueInProgress=false,sendQueue=[];
function setInputsDisabled(d){const a=document.querySelector('.attach-btn'),m=$('micBtn'),s=$('sendBtn');if(a){a.style.pointerEvents=d?'none':'';a.style.opacity=d?'0.5':'';}if(m){m.style.pointerEvents=d?'none':'';m.style.opacity=d?'0.5':'';}if(s)s.disabled=d||!activePid||!wsUp;}

window.triggerFileSelect=()=>{if(processingFiles)return;openAttachSheet();};

// ══════════════════════════════════════════════════════
// 📎 ATTACH SHEET — панель выбора медиа
// ══════════════════════════════════════════════════════
let _attachDragStartY=0;
let _attachDragCurrentY=0;
let _attachDragging=false;
let _attachSheetInited=false;

function openAttachSheet(){
  if(!activePid||isContactBlocked(activePid))return;
  // Если открыта панель стикеров — закрываем её первой
  const sp=$('stickerPanel');
  if(sp&&sp.classList.contains('open')){closeStickerPanel();}

  const sheet=$('attachSheet');
  const backdrop=$('attachBackdrop');
  const inputBar=$('chatInputBar');
  if(!sheet||!backdrop)return;

  // Сбрасываем анимацию items перед открытием (на случай повторного открытия)
  const grid=sheet.querySelector('.attach-grid');
  if(grid){
    grid.classList.remove('attach-items-animate');
    // Принудительный reflow — гарантирует сброс animation-fill-mode
    void grid.offsetWidth;
  }

  $('messageInput').blur();
  sheet.style.transform='';
  sheet.classList.remove('dragging');
  sheet.classList.add('open');
  backdrop.classList.add('open');
  if(inputBar)inputBar.classList.add('attach-mode');

  _initAttachSheetHandlers();

  // Запускаем анимацию items ПОСЛЕ завершения slide-in transition sheet-а.
  // В power-save режиме transition=0.15s, в обычном=0.32s.
  // Слушаем transitionend на sheet, с фоллбэком по таймауту.
  let _itemAnimFired=false;
  function _triggerItemAnim(){
    if(_itemAnimFired)return;
    _itemAnimFired=true;
    if(grid)grid.classList.add('attach-items-animate');
  }
  function _onSheetTransitionEnd(e){
    if(e.target!==sheet||e.propertyName!=='transform')return;
    sheet.removeEventListener('transitionend',_onSheetTransitionEnd);
    _triggerItemAnim();
  }
  sheet.addEventListener('transitionend',_onSheetTransitionEnd);
  // Фоллбэк: если transitionend не сработал (power-save, очень быстро, или анимация отключена)
  const _powerSave=document.body.classList.contains('power-save');
  setTimeout(_triggerItemAnim,_powerSave?80:340);

  setTimeout(()=>{document.addEventListener('click',_attachOutsideClick);},80);
}

function closeAttachSheet(){
  const sheet=$('attachSheet');
  const backdrop=$('attachBackdrop');
  const inputBar=$('chatInputBar');
  if(!sheet)return Promise.resolve();
  if(!sheet.classList.contains('open'))return Promise.resolve();

  // Сбрасываем класс анимации items перед закрытием
  const grid=sheet.querySelector('.attach-grid');
  if(grid)grid.classList.remove('attach-items-animate');

  sheet.classList.remove('open','dragging');
  sheet.style.transform='';
  if(backdrop)backdrop.classList.remove('open');
  if(inputBar)inputBar.classList.remove('attach-mode');
  document.removeEventListener('click',_attachOutsideClick);

  return new Promise(res=>setTimeout(res,340));
}

function _attachOutsideClick(e){
  const sheet=$('attachSheet');
  if(!sheet)return;
  if(!sheet.contains(e.target)){
    const ab=document.querySelector('.attach-btn');
    if(ab&&ab.contains(e.target))return;
    closeAttachSheet();
  }
}

function _initAttachSheetHandlers(){
  if(_attachSheetInited)return;
  _attachSheetInited=true;

  const sheet=$('attachSheet');
  const backdrop=$('attachBackdrop');
  const handle=$('attachSheetHandle');

  // ── Тап на фон закрывает панель ──
  if(backdrop){
    backdrop.addEventListener('click',()=>closeAttachSheet());
  }

  // ── Свайп вниз для закрытия (drag-to-close) ──
  function onDragStart(clientY){
    _attachDragStartY=clientY;
    _attachDragCurrentY=clientY;
    _attachDragging=true;
    sheet.classList.add('dragging');
  }
  function onDragMove(clientY){
    if(!_attachDragging)return;
    _attachDragCurrentY=clientY;
    const dy=Math.max(0,clientY-_attachDragStartY);
    sheet.style.transform=`translateY(${dy}px)`;
    // Затемнение фона уменьшается пропорционально свайпу
    const sheetH=sheet.offsetHeight||300;
    const ratio=Math.min(1,dy/sheetH);
    if(backdrop)backdrop.style.opacity=String(1-ratio);
  }
  function onDragEnd(){
    if(!_attachDragging)return;
    _attachDragging=false;
    sheet.classList.remove('dragging');
    const dy=Math.max(0,_attachDragCurrentY-_attachDragStartY);
    const sheetH=sheet.offsetHeight||300;
    if(backdrop)backdrop.style.opacity='';
    if(dy>sheetH*0.28){
      // Достаточно далеко — закрываем
      closeAttachSheet();
    }else{
      // Возвращаем на место
      sheet.style.transform='';
    }
  }

  // Touch
  handle.addEventListener('touchstart',e=>{onDragStart(e.touches[0].clientY);},{passive:true});
  handle.addEventListener('touchmove',e=>{onDragMove(e.touches[0].clientY);},{passive:true});
  handle.addEventListener('touchend',onDragEnd,{passive:true});
  // Mouse (desktop)
  handle.addEventListener('mousedown',e=>{
    onDragStart(e.clientY);
    const mm=ev=>onDragMove(ev.clientY);
    const mu=()=>{onDragEnd();document.removeEventListener('mousemove',mm);document.removeEventListener('mouseup',mu);};
    document.addEventListener('mousemove',mm);
    document.addEventListener('mouseup',mu);
  });

  // ── Также позволяем тянуть весь sheet за свободную область сверху ──
  // (handle уже покрывает это; доп. зона не нужна — handle достаточно широкая)

  // ── Кнопки выбора ──
  $('attachItemPhoto').onclick=()=>{
    closeAttachSheet();
    ignoreNextVisibilityReturn=true;$('mediaInput').click();
  };
  $('attachItemVideo').onclick=()=>{
    closeAttachSheet();
    ignoreNextVisibilityReturn=true;$('videoInput').click();
  };
  $('attachItemFile').onclick=()=>{
    closeAttachSheet();
    ignoreNextVisibilityReturn=true;$('fileInput').click();
  };
  $('attachItemSticker').onclick=async()=>{
    await closeAttachSheet();
    openStickerPanel();
  };
}

window.handleMediaSelect=async e=>{if(processingFiles)return;const files=Array.from(e.target.files);if(!files.length)return;$('mediaInput').value='';processingFiles=true;setInputsDisabled(true);showProcessingToast('Обработка фото…');try{for(const f of files){if(f.size>MAX_FILE_SIZE){toast('Файл слишком большой','err');continue;}if(f.type.startsWith('image/')){const c=await compressImage(f);pendingFiles.push({name:c.name,type:c.type,data:c.dataURL,size:f.size,blob:f,isMedia:true});}}}finally{processingFiles=false;setInputsDisabled(false);hideProgressToast();if(pendingFiles.length)updateFilePreview();}};
window.handleVideoSelect=async e=>{if(processingFiles)return;const files=Array.from(e.target.files);if(!files.length)return;$('videoInput').value='';processingFiles=true;setInputsDisabled(true);showProcessingToast('Подготовка видео…');try{for(const f of files){if(f.size>MAX_FILE_SIZE){toast('Файл слишком большой','err');continue;}pendingFiles.push({name:f.name,type:f.type,data:null,size:f.size,blob:f,isMedia:false,isVideo:true});}}finally{processingFiles=false;setInputsDisabled(false);hideProgressToast();if(pendingFiles.length)updateFilePreview();}};
window.handleFileSelect=async e=>{if(processingFiles)return;const files=Array.from(e.target.files);if(!files.length)return;$('fileInput').value='';processingFiles=true;setInputsDisabled(true);showProcessingToast('Обработка файлов…');try{for(const f of files){if(f.size>MAX_FILE_SIZE){toast('Файл слишком большой','err');continue;}pendingFiles.push({name:f.name,type:f.type,data:null,size:f.size,blob:f,isMedia:false});}}finally{processingFiles=false;setInputsDisabled(false);hideProgressToast();if(pendingFiles.length)updateFilePreview();}};

// ── SEND ──
async function sendSingleMsg(text,fileInfo=null,existingId=null){
  if(activePid===MY_ID){
    const msgId=existingId||uid(),ts=Date.now();
    const m={id:msgId,text,type:'sent',time:new Date(ts).toISOString(),reactions:{},edited:false,replyTo:replyTo||null,delivered:true,forwarded:false};
    if(fileInfo&&fileInfo.isMedia){m.media={type:fileInfo.type,data:fileInfo.data};m.caption=text;}
    else if(fileInfo){
      let data=fileInfo.data;
      if(!data&&fileInfo.blob){
        const ab=await readFileAsArrayBuffer(fileInfo.blob);
        data=`data:${fileInfo.type};base64,${arrayBufferToBase64(ab)}`;
      }
      if(data&&data.length>100000){
        try{
          const buf=base64ToArrayBuffer(data.replace(/^data:[^,]+,/,''));
          await saveBlob(msgId+'_file',buf);
          m.file={name:fileInfo.name,type:fileInfo.type,size:fileInfo.size,blobId:msgId+'_file'};
        }catch(e){m.file={name:fileInfo.name,type:fileInfo.type,size:fileInfo.size,data};}
      }else{
        m.file={name:fileInfo.name,type:fileInfo.type,size:fileInfo.size,data};
      }
    }
    await upsertMsg(MY_ID,m);const lp=fileInfo&&!fileInfo.isMedia?`${fileInfo.name}`:fileInfo&&fileInfo.isMedia?'Фото':text.slice(0,28);await updateChat(MY_ID,{lastMsg:lp,lastMsgTime:ts});appendOrReloadMsg(MY_ID,m);return;
  }

  const msgId=existingId||uid(),ts=Date.now();
  const targetPid = activePid;

  // Если это не переотправка существующего сообщения
  if(!existingId){
    const m={id:msgId,text,type:'sent',time:new Date(ts).toISOString(),reactions:{},edited:false,replyTo:replyTo||null,delivered:false,forwarded:false,_pending:!wsUp};
    if(fileInfo&&fileInfo.isMedia){m.media=fileInfo.media||{type:fileInfo.type,data:fileInfo.data};m.caption=text;}
    else if(fileInfo){m.file={name:fileInfo.name,type:fileInfo.type,size:fileInfo.size,blobId:msgId+'_file'};}
    
    await upsertMsg(targetPid,m);
    await updateChat(targetPid,{lastMsg:fileInfo&&fileInfo.isMedia?'Фото':text.slice(0,28),lastMsgTime:ts});
    appendOrReloadMsg(targetPid,m);
  }

  if(!wsUp){
    // Сохраняем в Outbox для автоматической отправки
    if(!outbox.some(o=>o.msgId===msgId)){
        outbox.push({type:fileInfo?'file':'msg', text, msgId, target: targetPid, fileInfo: fileInfo?{name:fileInfo.name, type:fileInfo.type, size:fileInfo.size}:null, ts});
        saveOutbox();
    }
    _markMsgPending(targetPid, msgId);
    return msgId;
  }

  const key=await ensureKey(targetPid);
  if(!fileInfo||fileInfo.isMedia){
    const envType=fileInfo&&fileInfo.isMedia?'media':'msg';
    const env={type:envType,id:msgId,text,ts,replyTo:replyTo||null,forwarded:false};
    if(fileInfo&&fileInfo.isMedia){env.media=fileInfo.media||{type:fileInfo.type,data:fileInfo.data};env.caption=text;}
    const enc=await encData(ENC.encode(JSON.stringify(env)),key);
    wsSend({type:'send-msg',target:targetPid,msgId,payload:payloadToB64(enc)});
    return msgId;
  }
  return sendFileToServer(fileInfo,text,msgId);
}

async function sendMsg(){
  if(processingFiles)return;
  const inp=$('messageInput'),text=inp.value.trim();
  if(pendingFiles.length>0){_vib('impactLight');sendQueue=pendingFiles.slice();pendingFiles=[];clearMediaPreview();inp.value='';inp.style.height='';cancelReply();processSendQueue(text);return;}
  if(!text)return;if(editId){_vib('impactLight');await commitEdit(text);return;}
  _vib('impactLight'); // тактильный отклик — сообщение отправлено
  await sendSingleMsg(text);inp.value='';inp.style.height='';cancelReply();
}
// ОПТИМИЗАЦИЯ 11: processSendQueue — обработка offline
async function processSendQueue(caption){if(queueInProgress)return;queueInProgress=true;setInputsDisabled(true);const total=sendQueue.length;for(let i=0;i<total;i++){const fi=sendQueue[i];try{await sendSingleMsg(caption,fi);}catch(e){if(!wsUp){toast(`Нет связи. Файл будет отправлен при подключении`,'warn');}else{toast(`Ошибка отправки ${fi.name}`,'err');}}}sendQueue=[];queueInProgress=false;setInputsDisabled(false);hideProgressToast();}

$('sendBtn').addEventListener('click',()=>sendMsg().catch(console.warn));
$('messageInput').addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendMsg().catch(console.warn);}});
$('messageInput').addEventListener('input',function(){
  this.style.height='auto';this.style.height=Math.min(this.scrollHeight,120)+'px';
  if(activePid&&wsUp&&keyCache[activePid]){sendTyping(true);clearTimeout(window._tyd);window._tyd=setTimeout(()=>sendTyping(false),30000);}
});
async function sendTyping(isTyping){const key=keyCache[activePid];if(!key||!wsUp)return;try{const enc=await encData(ENC.encode(JSON.stringify({type:'typing',isTyping,ts:Date.now()})),key);wsSend({type:'send-msg',target:activePid,msgId:uid(),payload:payloadToB64(enc),ephemeral:true});}catch(e){}}

let _typTmr=null;
function handleTyping(isTyping){
  clearTimeout(_typTmr);
  if(isTyping){
    // Typing использует старый подход (нет heartbeat для typing) — сбрасываем через 4с
    startPeerActivity('typing');
    // Переопределяем таймер из startPeerActivity на 4с (короче чем для activity)
    clearTimeout(_activityClearTimer);
    _activityClearTimer=setTimeout(()=>{stopPeerActivity();updateBar();},4000);
  }else{stopPeerActivity();updateBar();}
}

// ── REACTIONS ──
async function toggleReaction(msgId,emoji){
  let limitReached=false,resultReactions=null,notFound=false;
  await withChatLock(activePid,async()=>{
    const msgs=await loadMsgs(activePid);
    const m=msgs.find(m=>m.id===msgId);
    if(!m){notFound=true;return;}
    if(!m.reactions)m.reactions={};
    const cur=Object.keys(m.reactions).filter(k=>m.reactions[k].includes(MY_ID)).length;
    if(cur>=MAX_REACTIONS_PER_MSG&&!m.reactions[emoji]?.includes(MY_ID)){limitReached=true;return;}
    if(!m.reactions[emoji])m.reactions[emoji]=[];
    const idx=m.reactions[emoji].indexOf(MY_ID);
    if(idx===-1)m.reactions[emoji].push(MY_ID);else m.reactions[emoji].splice(idx,1);
    if(!m.reactions[emoji].length)delete m.reactions[emoji];
    await saveMsgs(activePid,msgs);
    resultReactions=m.reactions;
  });
  if(notFound)return;
  if(limitReached){toast(`Нельзя поставить больше ${MAX_REACTIONS_PER_MSG} реакций`,'warn');return;}
  _vib('tick');
  reRenderReactions(document.querySelector(`.msg-row[data-msgid="${msgId}"]`),resultReactions,msgId);
  if(activePid===MY_ID)return;
  const key=keyCache[activePid];if(!key)return;
  const enc=await encData(ENC.encode(JSON.stringify({type:'reaction',msgId,emoji,ts:Date.now()})),key);
  wsSend({type:'send-msg',target:activePid,msgId:uid(),payload:payloadToB64(enc)});
}
async function handleInReaction(from,msgId,emoji){await withChatLock(from,async()=>{const msgs=await loadMsgs(from);const m=msgs.find(m=>m.id===msgId);if(!m)return;if(!m.reactions)m.reactions={};if(!m.reactions[emoji])m.reactions[emoji]=[];const idx=m.reactions[emoji].indexOf(from);if(idx===-1)m.reactions[emoji].push(from);else m.reactions[emoji].splice(idx,1);if(!m.reactions[emoji].length)delete m.reactions[emoji];await saveMsgs(from,msgs);});if(activePid===from){const msgs=await loadMsgs(from);const m=msgs.find(m=>m.id===msgId);if(m)reRenderReactions(document.querySelector(`.msg-row[data-msgid="${msgId}"]`),m.reactions,msgId);}}
function reRenderReactions(row,reactions,msgId){if(!row)return;const os=row.querySelector('.reactions-strip');if(os)os.remove();if(!reactions||!Object.keys(reactions).some(k=>reactions[k].length>0))return;const strip=document.createElement('div');strip.className='reactions-strip';Object.entries(reactions).filter(([,v])=>v.length>0).forEach(([em,users])=>{const chip=document.createElement('div');chip.className='reaction-chip'+(users.includes(MY_ID)?' mine':'');chip.innerHTML=`${em}<span class="rc-count">${users.length}</span>`;chip.onclick=e=>{e.stopPropagation();toggleReaction(msgId,em);};strip.appendChild(chip);});row.appendChild(strip);}

// ── EDIT ──
let editId=null;
async function startEdit(msgId){const msgs=await loadMsgs(activePid);const m=msgs.find(m=>m.id===msgId);if(!m||m.media||m.voice||m.file||m.fileAvailable)return;editId=msgId;$('editBar').classList.add('visible');$('editBarPreview').textContent=m.text;const inp=$('messageInput');inp.value=m.text;inp.focus();inp.style.height='auto';inp.style.height=Math.min(inp.scrollHeight,120)+'px';$('sendBtn').innerHTML='<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M20 6 9 17l-5-5" /> </svg>';}
window.cancelEdit=()=>{editId=null;$('editBar').classList.remove('visible');$('messageInput').value='';$('messageInput').style.height='';$('sendBtn').innerHTML='<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M3.714 3.048a.498.498 0 0 0-.683.627l2.843 7.627a2 2 0 0 1 0 1.396l-2.842 7.627a.498.498 0 0 0 .682.627l18-8.5a.5.5 0 0 0 0-.904z" /> <path d="M6 12h16" /> </svg>';};
async function commitEdit(newText){await withChatLock(activePid,async()=>{const msgs=await loadMsgs(activePid);const m=msgs.find(m=>m.id===editId);if(!m)return;m.text=newText;m.edited=true;m.editedTs=Date.now();await saveMsgs(activePid,msgs);});const row=document.querySelector(`.msg-row[data-msgid="${editId}"]`);if(row){const bt=row.querySelector('.bubble-text');if(bt){bt.innerHTML=formatText(newText);if(!bt.querySelector('.msg-edited')){const ed=document.createElement('span');ed.className='msg-edited';ed.textContent=' ред.';bt.appendChild(ed);}}}await updateChat(activePid,{lastMsg:newText,lastMsgTime:Date.now()});if(activePid!==MY_ID){const key=keyCache[activePid];if(key){const enc=await encData(ENC.encode(JSON.stringify({type:'edit',msgId:editId,newText,ts:Date.now()})),key);wsSend({type:'send-msg',target:activePid,msgId:uid(),payload:payloadToB64(enc)});}}cancelEdit();}
async function handleInEdit(from,msgId,newText,ts){await withChatLock(from,async()=>{const msgs=await loadMsgs(from);const m=msgs.find(m=>m.id===msgId);if(!m)return;m.text=newText;m.edited=true;m.editedTs=ts||Date.now();await saveMsgs(from,msgs);});if(activePid===from){const row=document.querySelector(`.msg-row[data-msgid="${msgId}"]`);if(row){const bt=row.querySelector('.bubble-text');if(bt){bt.innerHTML=formatText(newText);if(!bt.querySelector('.msg-edited')){const ed=document.createElement('span');ed.className='msg-edited';ed.textContent=' ред.';bt.appendChild(ed);}}}}}

// ── DELETE ──
function deleteMsg(msgId){
  showConfirm({
    icon:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg>',
    title:'Удалить сообщение?',
    text:'Сообщение будет удалено без возможности восстановления.',
    yesLabel:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg> Удалить',
    noLabel:'Отмена',
    onYes: async ()=>{
      // Удаляем blob если есть
      const _msgs0=await loadMsgs(activePid);
      const _m0=_msgs0.find(m=>m.id===msgId);
      if(_m0){
        if(_m0.media?.blobId)deleteBlob(_m0.media.blobId);
        if(_m0.voice?.blobId)deleteBlob(_m0.voice.blobId);
        if(_m0.file?.blobId)deleteBlob(_m0.file.blobId);
      }
      await withChatLock(activePid,async()=>{const msgs=await loadMsgs(activePid);const idx=msgs.findIndex(m=>m.id===msgId);if(idx===-1)return;msgs.splice(idx,1);await saveMsgs(activePid,msgs);});const row=document.querySelector(`.msg-row[data-msgid="${msgId}"]`);if(row)(row.closest('.msg-row-outer')||row).remove();const msgs=await loadMsgs(activePid);const last=msgs[msgs.length-1];await updateChat(activePid,{lastMsg:last?.text||(last?.voice?'Голосовое':last?.media?'Фото':last?.file?`${last.file.name}`:''),lastMsgTime:last?.time?new Date(last.time).getTime():null});if(activePid!==MY_ID){const key=keyCache[activePid];if(key){const enc=await encData(ENC.encode(JSON.stringify({type:'delete',msgId,ts:Date.now()})),key);wsSend({type:'send-msg',target:activePid,msgId:uid(),payload:payloadToB64(enc)});}}
    }
  });
}
async function handleInDelete(from,msgId){await withChatLock(from,async()=>{const msgs=await loadMsgs(from);const idx=msgs.findIndex(m=>m.id===msgId);if(idx===-1)return;msgs.splice(idx,1);await saveMsgs(from,msgs);});if(activePid===from){const row=document.querySelector(`.msg-row[data-msgid="${msgId}"]`);if(row)(row.closest('.msg-row-outer')||row).remove();}}

// ══════════════════════════════════════════════════════
// ── АВТОУДАЛЕНИЕ СООБЩЕНИЙ (Telegram-стиль) ──
// ══════════════════════════════════════════════════════
// Хранение: localStorage bc_autodel_<peerId> → {ttl: ms|null, setBy: peerId, setAt: ts}
// TTL вариантов: null = выкл, 86400000 = 1 день, 604800000 = 7 дней, 2592000000 = 1 мес

const AUTODEL_QUICK_OPTIONS=[
  {key:'off',icon:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <line x1=\"10\" x2=\"14\" y1=\"2\" y2=\"2\" /> <line x1=\"12\" x2=\"15\" y1=\"14\" y2=\"11\" /> <circle cx=\"12\" cy=\"14\" r=\"8\" /> </svg>',label:'Выключить',ttl:null},
  {key:'1d', icon:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M12 6v6l4 2\" /> </svg>',label:'1 день',   ttl:86400000},
  {key:'7d', icon:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M8 2v4\" /> <path d=\"M16 2v4\" /> <rect width=\"18\" height=\"18\" x=\"3\" y=\"4\" rx=\"2\" /> <path d=\"M3 10h18\" /> <path d=\"M8 14h.01\" /> <path d=\"M12 14h.01\" /> <path d=\"M16 14h.01\" /> <path d=\"M8 18h.01\" /> <path d=\"M12 18h.01\" /> <path d=\"M16 18h.01\" /> </svg>',label:'7 дней',   ttl:604800000},
  {key:'1mo',icon:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M8 2v4\" /> <path d=\"M16 2v4\" /> <rect width=\"18\" height=\"18\" x=\"3\" y=\"4\" rx=\"2\" /> <path d=\"M3 10h18\" /> <path d=\"M8 14h.01\" /> <path d=\"M12 14h.01\" /> <path d=\"M16 14h.01\" /> <path d=\"M8 18h.01\" /> <path d=\"M12 18h.01\" /> <path d=\"M16 18h.01\" /> </svg>',label:'1 месяц',  ttl:2592000000},
];

// Полный список для кастомного пикера "Автоудаление через..." (Telegram-стиль)
const _DAY=86400000;
const AUTODEL_CUSTOM_OPTIONS=[
  {key:'none', label:'Нет',       ttl:null},
  {key:'1d',   label:'1 день',    ttl:1*_DAY},
  {key:'2d',   label:'2 дня',     ttl:2*_DAY},
  {key:'3d',   label:'3 дня',     ttl:3*_DAY},
  {key:'4d',   label:'4 дня',     ttl:4*_DAY},
  {key:'5d',   label:'5 дней',    ttl:5*_DAY},
  {key:'6d',   label:'6 дней',    ttl:6*_DAY},
  {key:'1w',   label:'1 нед.',    ttl:7*_DAY},
  {key:'2w',   label:'2 нед.',    ttl:14*_DAY},
  {key:'3w',   label:'3 нед.',    ttl:21*_DAY},
  {key:'1mo',  label:'1 месяц',   ttl:30*_DAY},
  {key:'2mo',  label:'2 месяца',  ttl:60*_DAY},
  {key:'3mo',  label:'3 месяца',  ttl:90*_DAY},
  {key:'4mo',  label:'4 месяца',  ttl:120*_DAY},
  {key:'5mo',  label:'5 месяцев', ttl:150*_DAY},
  {key:'6mo',  label:'6 месяцев', ttl:180*_DAY},
  {key:'1y',   label:'1 год',     ttl:365*_DAY},
];

function _autoDelKey(pid){return`bc_autodel_${pid}`;}

function getAutoDelState(pid){
  return lsGet(_autoDelKey(pid),{ttl:null,key:'off',setBy:null,setAt:0});
}

// Определяем "ключ" варианта по ttl (для обратной совместимости со старыми
// сохранёнными состояниями, где поля key ещё не было)
function _keyForTtl(ttl){
  if(!ttl)return'off';
  const q=AUTODEL_QUICK_OPTIONS.find(o=>o.ttl===ttl);
  if(q)return q.key;
  const c=AUTODEL_CUSTOM_OPTIONS.find(o=>o.ttl===ttl);
  if(c)return c.key;
  return'off';
}

// Возвращаем подпись варианта. key имеет приоритет (различает дубликаты
// ttl, напр. "7 дней" из быстрого меню и "1 нед." из кастомного пикера —
// оба ttl=604800000, но разные подписи)
function _autoDelLabel(ttl,key){
  if(!ttl)return null;
  if(key){
    const c=AUTODEL_CUSTOM_OPTIONS.find(o=>o.key===key&&o.ttl===ttl);
    if(c)return c.label;
    const q=AUTODEL_QUICK_OPTIONS.find(o=>o.key===key&&o.ttl===ttl);
    if(q)return q.label;
  }
  const q2=AUTODEL_QUICK_OPTIONS.find(o=>o.ttl===ttl);
  if(q2)return q2.label;
  const c2=AUTODEL_CUSTOM_OPTIONS.find(o=>o.ttl===ttl);
  return c2?c2.label:null;
}

// Строим системное сообщение в DOM
function buildSysMsg(text){
  const wrap=document.createElement('div');
  wrap.className='sys-msg';
  const pill=document.createElement('div');
  pill.className='sys-msg-pill';
  pill.textContent=text;
  wrap.appendChild(pill);
  return wrap;
}

// Добавляем системное сообщение в чат и сохраняем в историю
async function _insertSysMsg(pid, text){
  // Сохраняем в историю как служебный тип
  const sysRecord={
    id:uid(),
    type:'system',
    text,
    time:new Date().toISOString()
  };
  await upsertMsg(pid, sysRecord);
  // Если чат открыт — показываем сразу
  if(activePid===pid){
    const area=$('messagesArea');
    if(area){
      const el=buildSysMsg(text);
      area.appendChild(el);
      area.scrollTop=area.scrollHeight;
    }
  }
}

// Обновляем кнопку в шапке
function _updateAutoDelBtn(pid){
  const btn=$('autoDelBtn');
  if(!btn)return;
  const st=getAutoDelState(pid||activePid);
  const stKey=st.key||_keyForTtl(st.ttl);
  btn.classList.toggle('autodel-active',!!st.ttl);
  btn.title=st.ttl?`Автоудаление: ${_autoDelLabel(st.ttl,stKey)}`:'Автоудаление сообщений';
}

// Открыть шторку выбора времени
window.openAutoDelSheet=function(){
  if(!activePid)return;
  const st=getAutoDelState(activePid);
  const stKey=st.key||_keyForTtl(st.ttl);
  const sheet=$('autoDelSheet');
  const opts=$('autoDelOptions');
  if(!sheet||!opts)return;
  opts.innerHTML='';

  // Если включён вариант не из быстрых 4 (выбран через "⚙️ Настроить") —
  // показываем его отдельной строкой ВВЕРХУ списка с галочкой (как в Telegram)
  const isCustomSelected = !!st.ttl && !AUTODEL_QUICK_OPTIONS.some(o=>o.key===stKey);
  if(isCustomSelected){
    const customLabel=_autoDelLabel(st.ttl,stKey)||'';
    const div=document.createElement('div');
    div.className='autodel-opt selected';
    div.innerHTML=`<span class="autodel-opt-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <line x1="10" x2="14" y1="2" y2="2" /> <line x1="12" x2="15" y1="14" y2="11" /> <circle cx="12" cy="14" r="8" /> </svg></span><span class="autodel-opt-label">${customLabel}</span><span class="autodel-opt-check"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M20 6 9 17l-5-5" /> </svg></span>`;
    div.onclick=()=>{closeAutoDelSheet();};
    opts.appendChild(div);
    const div0=document.createElement('div');
    div0.className='autodel-divider';
    opts.appendChild(div0);
  }

  AUTODEL_QUICK_OPTIONS.forEach((opt,i)=>{
    const isSelected = !isCustomSelected && opt.key===stKey;
    const div=document.createElement('div');
    div.className='autodel-opt'+(isSelected?' selected':'');
    div.innerHTML=`<span class="autodel-opt-icon">${opt.icon}</span><span class="autodel-opt-label">${opt.label}</span><span class="autodel-opt-check"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M20 6 9 17l-5-5" /> </svg></span>`;
    div.onclick=()=>{closeAutoDelSheet();_applyAutoDelChoice(opt.ttl,opt.key);};
    opts.appendChild(div);
    if(i===0){
      const div2=document.createElement('div');
      div2.className='autodel-divider';
      opts.appendChild(div2);
    }
  });

  // Разделитель + кнопка "⚙️ Настроить" — открывает кастомный пикер
  const div3=document.createElement('div');
  div3.className='autodel-divider';
  opts.appendChild(div3);

  const gear=document.createElement('div');
  gear.className='autodel-opt autodel-opt-gear';
  gear.innerHTML=`<span class="autodel-opt-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M9.671 4.136a2.34 2.34 0 0 1 4.659 0 2.34 2.34 0 0 0 3.319 1.915 2.34 2.34 0 0 1 2.33 4.033 2.34 2.34 0 0 0 0 3.831 2.34 2.34 0 0 1-2.33 4.033 2.34 2.34 0 0 0-3.319 1.915 2.34 2.34 0 0 1-4.659 0 2.34 2.34 0 0 0-3.32-1.915 2.34 2.34 0 0 1-2.33-4.033 2.34 2.34 0 0 0 0-3.831A2.34 2.34 0 0 1 6.35 6.051a2.34 2.34 0 0 0 3.319-1.915" /> <circle cx="12" cy="12" r="3" /> </svg></span><span class="autodel-opt-label">Настроить</span>`;
  gear.onclick=()=>{
    closeAutoDelSheet();
    setTimeout(()=>openAutoDelCustomSheet(),260);
  };
  opts.appendChild(gear);

  const titleEl=$('autoDelTitle');
  if(titleEl){
    const lbl=_autoDelLabel(st.ttl,stKey);
    titleEl.innerHTML=lbl
      ?`<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <line x1="10" x2="14" y1="2" y2="2" /> <line x1="12" x2="15" y1="14" y2="11" /> <circle cx="12" cy="14" r="8" /> </svg> АВТОУДАЛЕНИЕ · ${lbl.toUpperCase()}`
      :'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <line x1=\"10\" x2=\"14\" y1=\"2\" y2=\"2\" /> <line x1=\"12\" x2=\"15\" y1=\"14\" y2=\"11\" /> <circle cx=\"12\" cy=\"14\" r=\"8\" /> </svg> АВТОУДАЛЕНИЕ СООБЩЕНИЙ';
  }

  sheet.classList.add('open');
};

window.closeAutoDelSheet=function(){
  const sheet=$('autoDelSheet');
  if(sheet)sheet.classList.remove('open');
};

// ── КАСТОМНЫЙ ПИКЕР "Автоудаление через..." (Telegram-стиль) ─────────────────
const AUTODEL_ROW_H=44; // высота одной строки пикера (синхронизирована с CSS)
let _autoDelPickerIdx=0;

window.openAutoDelCustomSheet=function(){
  if(!activePid)return;
  const sheet=$('autoDelCustomSheet');
  const scroll=$('autoDelPickerScroll');
  if(!sheet||!scroll)return;

  const st=getAutoDelState(activePid);
  const stKey=st.key||_keyForTtl(st.ttl);

  // Находим индекс текущего варианта в полном списке
  let initIdx=AUTODEL_CUSTOM_OPTIONS.findIndex(o=>o.key===stKey);
  if(initIdx<0)initIdx=AUTODEL_CUSTOM_OPTIONS.findIndex(o=>o.ttl===st.ttl);
  if(initIdx<0)initIdx=0;

  // Строим строки пикера
  scroll.innerHTML='';
  const padTop=document.createElement('div');padTop.className='autodel-picker-pad';
  scroll.appendChild(padTop);
  AUTODEL_CUSTOM_OPTIONS.forEach((opt,i)=>{
    const row=document.createElement('div');
    row.className='autodel-picker-row'+(i===initIdx?' center':'');
    row.textContent=opt.label;
    row.dataset.idx=i;
    row.onclick=()=>{_autoDelPickerScrollTo(i,true);};
    scroll.appendChild(row);
  });
  const padBot=document.createElement('div');padBot.className='autodel-picker-pad';
  scroll.appendChild(padBot);

  _autoDelPickerIdx=initIdx;
  _updateAutoDelCustomBtn(initIdx);
  _initAutoDelPickerScroll();

  // Кнопка снизу — применяет выбранный вариант (привязываем один раз)
  const btn=$('autoDelCustomBtn');
  if(btn&&!btn._bound){
    btn._bound=true;
    btn.onclick=()=>_applyAutoDelCustomChoice();
  }

  // Ставим начальную позицию без анимации
  scroll.scrollTop=initIdx*AUTODEL_ROW_H;

  sheet.classList.add('open');
};

window.closeAutoDelCustomSheet=function(){
  const sheet=$('autoDelCustomSheet');
  if(sheet)sheet.classList.remove('open');
};

function _autoDelPickerScrollTo(idx,smooth){
  const scroll=$('autoDelPickerScroll');
  if(!scroll)return;
  idx=Math.max(0,Math.min(AUTODEL_CUSTOM_OPTIONS.length-1,idx));
  scroll.scrollTo({top:idx*AUTODEL_ROW_H,behavior:smooth?'smooth':'auto'});
}

// Обновляем подпись кнопки внизу пикера в зависимости от выбранной строки
function _updateAutoDelCustomBtn(idx){
  const btn=$('autoDelCustomBtn');
  if(!btn)return;
  const opt=AUTODEL_CUSTOM_OPTIONS[idx];
  if(!opt)return;
  if(opt.ttl){
    btn.textContent='Включить удаление по таймеру';
    btn.classList.remove('btn-disable');
  }else{
    btn.textContent='Отключить автоудаление';
    btn.classList.add('btn-disable');
  }
}

// Слушаем скролл пикера — определяем центральную строку и обновляем подсветку+кнопку
function _initAutoDelPickerScroll(){
  const scroll=$('autoDelPickerScroll');
  if(!scroll||scroll._inited)return;
  scroll._inited=true;
  let raf=null;
  scroll.addEventListener('scroll',()=>{
    if(raf)return;
    raf=requestAnimationFrame(()=>{
      raf=null;
      const idx=Math.round(scroll.scrollTop/AUTODEL_ROW_H);
      const clamped=Math.max(0,Math.min(AUTODEL_CUSTOM_OPTIONS.length-1,idx));
      if(clamped!==_autoDelPickerIdx){
        _autoDelPickerIdx=clamped;
        scroll.querySelectorAll('.autodel-picker-row').forEach((r,i)=>{
          r.classList.toggle('center',i===clamped);
        });
        _updateAutoDelCustomBtn(clamped);
      }
    });
  },{passive:true});
}

// Применить выбор из кастомного пикера (кнопка снизу)
function _applyAutoDelCustomChoice(){
  const opt=AUTODEL_CUSTOM_OPTIONS[_autoDelPickerIdx];
  if(!opt)return;
  closeAutoDelCustomSheet();
  _applyAutoDelChoice(opt.ttl,opt.key);
}

// Применяем выбранный вариант
async function _applyAutoDelChoice(newTtl,newKey){
  if(!activePid)return;
  const oldSt=getAutoDelState(activePid);
  const oldKey=oldSt.key||_keyForTtl(oldSt.ttl);
  const effKey=newKey||_keyForTtl(newTtl);
  if(oldSt.ttl===newTtl&&oldKey===effKey)return; // ничего не изменилось

  // Сохраняем локально
  lsSet(_autoDelKey(activePid),{ttl:newTtl,key:effKey,setBy:MY_ID,setAt:Date.now()});
  _updateAutoDelBtn(activePid);

  // Системное сообщение
  const label=_autoDelLabel(newTtl,effKey);
  const sysText=newTtl
    ?`${_whoName(MY_ID)} включил(а) автоудаление сообщений через ${label}`
    :`${_whoName(MY_ID)} выключил(а) автоудаление сообщений`;
  await _insertSysMsg(activePid, sysText);

  // Запускаем/останавливаем таймер
  _scheduleAutoDelTimer(activePid, newTtl);

  // Синхронизируем с собеседником
  await _sendAutoDelSignal(activePid, newTtl, effKey);
}

// Имя для системного сообщения
function _whoName(id){
  if(id===MY_ID){
    const me=lsGet('bc_my_name',null);
    return me||'Вы';
  }
  const ct=loadContacts().find(c=>c.id===id);
  return ct?.name||id;
}

// Посылаем зашифрованный сигнал автоудаления собеседнику
async function _sendAutoDelSignal(pid, ttl, key){
  if(!wsUp||pid===MY_ID)return;
  const cryptoKey=keyCache[pid]||await loadPersistedKey(pid);
  if(!cryptoKey)return;
  try{
    const payload={type:'autodel-set',ttl:ttl||null,key:key||null,setBy:MY_ID,ts:Date.now()};
    const enc=await encData(ENC.encode(JSON.stringify(payload)),cryptoKey);
    wsSend({type:'send-msg',target:pid,msgId:uid(),payload:payloadToB64(enc)});
  }catch(e){}
}

// Обработка входящего сигнала (вызывается из handleEnvelope)
async function handleInAutoDelSet(from, env){
  const newKey=env.key||_keyForTtl(env.ttl||null);
  lsSet(_autoDelKey(from),{ttl:env.ttl||null,key:newKey,setBy:from,setAt:env.ts||Date.now()});

  if(activePid===from) _updateAutoDelBtn(from);

  const label=_autoDelLabel(env.ttl,newKey);
  const sysText=(env.ttl)
    ?`${_whoName(from)} включил(а) автоудаление сообщений через ${label}`
    :`${_whoName(from)} выключил(а) автоудаление сообщений`;
  await _insertSysMsg(from, sysText);

  _scheduleAutoDelTimer(from, env.ttl||null);
}

// ── Таймер автоудаления ─────────────────────────────────────────────────────
const _autoDelTimers=new Map(); // pid → intervalId

function _scheduleAutoDelTimer(pid, ttl){
  // Останавливаем старый
  if(_autoDelTimers.has(pid)){
    clearInterval(_autoDelTimers.get(pid));
    _autoDelTimers.delete(pid);
  }
  if(!ttl)return;
  // Проверяем и удаляем просроченные сообщения каждые 60 сек
  const id=setInterval(()=>_runAutoDelSweep(pid,ttl),30000);
  _autoDelTimers.set(pid,id);
  // Сразу первый прогон
  _runAutoDelSweep(pid,ttl);
}

async function _runAutoDelSweep(pid, ttl){
  const st=getAutoDelState(pid);
  if(!st.ttl)return; // уже выключено
  const cutoff=Date.now()-ttl;
  await withChatLock(pid,async()=>{
    const msgs=await loadMsgs(pid);
    const toDelete=msgs.filter(m=>
      m.type!=='system' &&
      new Date(m.time).getTime()<cutoff
    );
    if(!toDelete.length)return;
    const remaining=msgs.filter(m=>
      m.type==='system'||new Date(m.time).getTime()>=cutoff
    );
    await saveMsgs(pid,remaining);
    // Удаляем blob-данные
    for(const m of toDelete){
      if(m.media?.blobId)deleteBlob(m.media.blobId).catch(()=>{});
      if(m.voice?.blobId)deleteBlob(m.voice.blobId).catch(()=>{});
      if(m.file?.blobId)deleteBlob(m.file.blobId).catch(()=>{});
    }
    // Если чат открыт — убираем из DOM
    if(activePid===pid){
      for(const m of toDelete){
        const row=document.querySelector(`.msg-row[data-msgid="${m.id}"]`);
        if(row)(row.closest('.msg-row-outer')||row).remove();
      }
    }
    // Обновляем превью чата
    const last=remaining.filter(m=>m.type!=='system').pop();
    await updateChat(pid,{
      lastMsg:last?.text||(last?.voice?'Голосовое':last?.media?'Фото':last?.file?`${last.file.name}`:''),
      lastMsgTime:last?.time?new Date(last.time).getTime():null
    });
  });
}

// Восстанавливаем таймеры при старте приложения
function _restoreAutoDelTimers(){
  // Перебираем все ключи bc_autodel_*
  for(let i=0;i<localStorage.length;i++){
    const k=localStorage.key(i);
    if(!k||!k.startsWith('bc_autodel_'))continue;
    const pid=k.replace('bc_autodel_','');
    const st=lsGet(k,{ttl:null});
    if(st.ttl)_scheduleAutoDelTimer(pid,st.ttl);
  }
}
_restoreAutoDelTimers();

// ── Инициализация кнопки при открытии чата ──────────────────────────────────
function initAutoDelBtn(pid){
  _updateAutoDelBtn(pid);
  const btn=$('autoDelBtn');
  // Скрываем кнопку в "Избранном" (MY_ID чат с собой)
  if(btn)btn.style.display=(pid===MY_ID)?'none':'flex';
}

// ── REPLY ──
let replyTo=null;
async function startReply(msgId){const msgs=await loadMsgs(activePid);const m=msgs.find(m=>m.id===msgId);if(!m)return;replyTo={id:m.id,text:(m.sticker?`${m.sticker} Стикер`:m.fileAvailable?`${m.fileAvailable.name}`:m.file?`${m.file.name}`:m.voice?'Голосовое':m.media?'Фото':m.text||'')};$('replyBarPreview').textContent=replyTo.text.slice(0,80);$('replyBar').classList.add('visible');$('messageInput').focus();}
window.cancelReply=()=>{replyTo=null;$('replyBar').classList.remove('visible');};

// ══════════════════════════════════════════════════════
// ── DATE SEPARATORS & FLOATING DATE ──
// ══════════════════════════════════════════════════════
function fmtDateLabel(ts){
  const d=new Date(ts);
  const now=new Date();
  const opts={day:'numeric',month:'long'};
  if(d.getFullYear()!==now.getFullYear())opts.year='numeric';
  return d.toLocaleDateString('ru',opts);
}

function isSameDay(ts1,ts2){
  const a=new Date(ts1),b=new Date(ts2);
  return a.getFullYear()===b.getFullYear()&&a.getMonth()===b.getMonth()&&a.getDate()===b.getDate();
}

function buildDateSeparator(ts){
  const wrap=document.createElement('div');
  wrap.className='date-separator';
  wrap.dataset.dateTs=String(ts);
  const pill=document.createElement('div');
  pill.className='date-separator-pill';
  pill.textContent=fmtDateLabel(ts);
  wrap.appendChild(pill);
  return wrap;
}

// ══════════════════════════════════════════════════════
// ── FLOATING DATE (Telegram-style) ──
// position:fixed — поверх всего, по центру горизонтально.
// Показывается при начале скролла, исчезает через 1.2с после остановки.
// ══════════════════════════════════════════════════════
let _floatDateHideTimer=null;
let _floatDateVisible=false;

// Вычисляет top для floatingDate: верхний край messagesArea + небольшой отступ
function _positionFloatingDate(){
  const area=$('messagesArea');
  const floatEl=$('floatingDate');
  if(!area||!floatEl)return;
  const r=area.getBoundingClientRect();
  floatEl.style.top=(r.top+8)+'px';
}

// Находит текущую отображаемую дату по положению сепараторов
function _getCurrentDateTs(){
  const area=$('messagesArea');
  if(!area)return null;
  const separators=area.querySelectorAll('.date-separator');
  if(!separators.length)return null;
  const areaRect=area.getBoundingClientRect();
  // Берём последний сепаратор, чей верхний край прокрутился выше середины области
  const threshold=areaRect.top + areaRect.height * 0.5;
  let bestTs=null;
  for(const sep of separators){
    const r=sep.getBoundingClientRect();
    const ts=parseInt(sep.dataset.dateTs);
    if(!ts)continue;
    if(r.top<=threshold){
      if(!bestTs||ts>bestTs)bestTs=ts;
    }
  }
  // Ничего не прокрутилось — показываем дату первого сепаратора
  if(!bestTs){
    const ts=parseInt(separators[0].dataset.dateTs);
    if(ts)bestTs=ts;
  }
  return bestTs;
}

function showFloatingDate(){
  const floatEl=$('floatingDate');
  const floatText=$('floatingDateText');
  if(!floatEl||!floatText)return;

  const ts=_getCurrentDateTs();
  if(!ts)return;

  // Обновляем позицию и текст
  _positionFloatingDate();
  floatText.textContent=fmtDateLabel(ts);

  // Показываем немедленно
  if(!_floatDateVisible){
    _floatDateVisible=true;
    floatEl.classList.add('visible');
  }

  // Сбрасываем таймер скрытия — пока скроллим, таймер не срабатывает
  clearTimeout(_floatDateHideTimer);
  // Скрываем через 1.2с после последнего scroll-события
  _floatDateHideTimer=setTimeout(()=>{
    _floatDateVisible=false;
    floatEl.classList.remove('visible');
  },1200);
}

function initFloatingDate(){
  const floatEl=$('floatingDate');
  if(!floatEl)return;
  _floatDateVisible=false;
  floatEl.classList.remove('visible');
  _positionFloatingDate();
}

function observeDateSeparator(_el){/* poll-based, не нужно */}
function updateFloatingDate(){showFloatingDate();}

// ── BUILD ROW ──
function mkMeta(m,isSent){const meta=document.createElement('span');meta.className='msg-meta';const timeEl=document.createElement('span');timeEl.className='msg-time-inline';timeEl.textContent=new Date(m.time).toLocaleTimeString('ru',{hour:'2-digit',minute:'2-digit'});meta.appendChild(timeEl);if(isSent){const tk=document.createElement('span');tk.className='msg-ticks'+(m.delivered?' double':' single');tk.innerHTML = m.delivered?'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M18 6 7 17l-5-5" /> <path d="m22 10-7.5 7.5L13 16" /> </svg>':'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M20 6 9 17l-5-5" /> </svg>';meta.appendChild(tk);}return meta;}

function buildMediaUploadSpinner(fileId){
  const wrap=document.createElement('div');wrap.className='upload-overlay';
  const spinnerWrap=document.createElement('div');spinnerWrap.style.cssText='position:relative;width:52px;height:52px;';
  const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 52 52');svg.classList.add('upload-spinner-svg');svg.style.cssText='position:absolute;inset:0;width:52px;height:52px;';
  const track=document.createElementNS('http://www.w3.org/2000/svg','circle');track.setAttribute('cx','26');track.setAttribute('cy','26');track.setAttribute('r','21');track.classList.add('upload-spinner-track');
  const fill=document.createElementNS('http://www.w3.org/2000/svg','circle');fill.setAttribute('cx','26');fill.setAttribute('cy','26');fill.setAttribute('r','21');fill.classList.add('upload-spinner-fill');
  svg.appendChild(track);svg.appendChild(fill);
  const icon=document.createElement('div');icon.className='upload-spinner-icon';icon.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 3v12\" /> <path d=\"m17 8-5-5-5 5\" /> <path d=\"M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4\" /> </svg>';
  spinnerWrap.appendChild(svg);spinnerWrap.appendChild(icon);
  const pct=document.createElement('div');pct.className='upload-pct';pct.textContent='0%';
  wrap.appendChild(spinnerWrap);wrap.appendChild(pct);
  registerUploadUI(fileId,spinnerWrap,fill,pct);
  return wrap;
}

function buildFileUploadSpinnerEl(fileId){
  const spinnerEl=document.createElement('div');spinnerEl.className='file-upload-spinner';
  const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 46 46');
  const track=document.createElementNS('http://www.w3.org/2000/svg','circle');track.setAttribute('cx','23');track.setAttribute('cy','23');track.setAttribute('r','21');track.classList.add('file-upload-spinner-track');
  const fill=document.createElementNS('http://www.w3.org/2000/svg','circle');fill.setAttribute('cx','23');fill.setAttribute('cy','23');fill.setAttribute('r','21');fill.classList.add('file-upload-spinner-fill');
  svg.appendChild(track);svg.appendChild(fill);
  const icon=document.createElement('div');icon.className='file-upload-spinner-icon';icon.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 3v12\" /> <path d=\"m17 8-5-5-5 5\" /> <path d=\"M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4\" /> </svg>';
  spinnerEl.appendChild(svg);spinnerEl.appendChild(icon);
  registerUploadUI(fileId,spinnerEl,fill,null);
  return spinnerEl;
}

// ── Вспомогательная: inline-мета (время + галочки) для нижней строки TG-file ──
function buildTgFileMeta(m, isSent){
  const ov = document.createElement('div');
  ov.className = 'tg-file-meta-inline';
  const tEl = document.createElement('span');
  tEl.className = 'msg-time-inline';
  tEl.textContent = new Date(m.time||Date.now()).toLocaleTimeString('ru',{hour:'2-digit',minute:'2-digit'});
  ov.appendChild(tEl);
  if(isSent){
    const tk = document.createElement('span');
    tk.className = 'msg-ticks'+(m.delivered?' double':' single');
    tk.innerHTML = m.delivered?'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M18 6 7 17l-5-5" /> <path d="m22 10-7.5 7.5L13 16" /> </svg>':'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M20 6 9 17l-5-5" /> </svg>';
    ov.appendChild(tk);
  }
  return ov;
}

// ── Telegram-стиль: файл уже скачан / у отправителя ──
function buildFileCardStatic(file, m, isSent){
  const wrap = document.createElement('div');
  wrap.className = 'tg-file-wrap';

  const icon = document.createElement('div');
  icon.className = 'tg-file-icon tg-file-icon-static';
  icon.innerHTML = getFileIcon(file.type);
  wrap.appendChild(icon);

  const info = document.createElement('div');
  info.className = 'tg-file-info';

  const name = document.createElement('div');
  name.className = 'tg-file-name';
  name.textContent = file.name;
  info.appendChild(name);

  // Нижняя строка: размер · тип · spacer · время · галочки — всё в одну линию
  const bottomRow = document.createElement('div');
  bottomRow.className = 'tg-file-bottom';

  const sz = document.createElement('span');
  sz.className = 'tg-file-size';
  sz.textContent = formatSize(file.size);
  bottomRow.appendChild(sz);

  const ext = (file.name||'').split('.').pop().toUpperCase().slice(0,6);
  if(ext){
    const badge = document.createElement('span');
    badge.className = 'tg-file-type-badge';
    badge.textContent = ext;
    bottomRow.appendChild(badge);
  }

  const spacer = document.createElement('span');
  spacer.className = 'tg-file-spacer';
  bottomRow.appendChild(spacer);

  if(m) bottomRow.appendChild(buildTgFileMeta(m, isSent));

  info.appendChild(bottomRow);
  wrap.appendChild(info);

  return wrap;
}

// ── Telegram-стиль: файл загружается (отправитель) ──
function buildFileCardUploading(file, fileId, m, isSent){
  const wrap = document.createElement('div');
  wrap.className = 'tg-file-wrap';

  // Спиннер загрузки
  const spinnerEl = document.createElement('div');
  spinnerEl.className = 'file-upload-spinner';
  const svg = document.createElementNS('http://www.w3.org/2000/svg','svg');
  svg.setAttribute('viewBox','0 0 42 42');
  const track = document.createElementNS('http://www.w3.org/2000/svg','circle');
  track.setAttribute('class','file-upload-spinner-track');
  track.setAttribute('cx','21');track.setAttribute('cy','21');track.setAttribute('r','19');
  const fill = document.createElementNS('http://www.w3.org/2000/svg','circle');
  fill.setAttribute('class','file-upload-spinner-fill');
  fill.setAttribute('cx','21');fill.setAttribute('cy','21');fill.setAttribute('r','19');
  svg.appendChild(track);svg.appendChild(fill);
  const iconEl = document.createElement('div');
  iconEl.className = 'file-upload-spinner-icon';
  iconEl.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 3v12\" /> <path d=\"m17 8-5-5-5 5\" /> <path d=\"M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4\" /> </svg>';
  spinnerEl.appendChild(svg);spinnerEl.appendChild(iconEl);
  registerUploadUI(fileId, spinnerEl, fill, null);
  wrap.appendChild(spinnerEl);

  const info = document.createElement('div');
  info.className = 'tg-file-info';
  const name = document.createElement('div');
  name.className = 'tg-file-name';
  name.textContent = file.name;
  info.appendChild(name);

  // Нижняя строка: "Загрузка…" · spacer · время · галочки
  const bottomRow = document.createElement('div');
  bottomRow.className = 'tg-file-bottom';
  const sz = document.createElement('span');
  sz.className = 'tg-file-size';
  sz.textContent = 'Загрузка…';
  bottomRow.appendChild(sz);
  const spacer = document.createElement('span');
  spacer.className = 'tg-file-spacer';
  bottomRow.appendChild(spacer);
  if(m) bottomRow.appendChild(buildTgFileMeta(m, isSent));
  info.appendChild(bottomRow);
  wrap.appendChild(info);

  return wrap;
}

// ── Telegram-стиль: файл доступен для скачивания (получатель) ──
function buildFileAvailableCard(m){
  const fa = m.fileAvailable;
  const isSent = m.type === 'sent';

  const isAudio = fa.mimeType && fa.mimeType.startsWith('audio/');
  const isVideo = fa.mimeType && fa.mimeType.startsWith('video/');
  const isImage = fa.mimeType && fa.mimeType.startsWith('image/');

  // ОПТИМИЗАЦИЯ 1: если есть thumb — строим враппер с превью
  // Показывается мгновенно, ещё до загрузки чанков
  if((isImage||isVideo)&&fa.thumb){
    const wrap=document.createElement('div');
    wrap.className='tg-file-wrap tg-file-wrap-thumb';
    wrap.style.cssText='position:relative;overflow:hidden;border-radius:12px;cursor:pointer;';
    // Превью-картинка с blur-эффектом (Telegram-стиль)
    const thumbImg=document.createElement('img');
    thumbImg.src=fa.thumb;
    thumbImg.style.cssText='width:100%;height:auto;min-height:80px;object-fit:cover;filter:blur(4px) brightness(0.7);transform:scale(1.1);display:block;border-radius:12px;';
    wrap.appendChild(thumbImg);
    // Оверлей с кнопкой скачивания
    const overlay=document.createElement('div');
    overlay.style.cssText='position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px;';
    const dlBtn=document.createElement('div');
    dlBtn.className='tg-file-icon tg-file-icon-dl';
    dlBtn.style.cssText='background:rgba(0,0,0,0.55);backdrop-filter:blur(4px);';
    dlBtn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 15V3\" /> <path d=\"M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4\" /> <path d=\"m7 10 5 5 5-5\" /> </svg>';
    dlBtn.onclick=e=>{e.stopPropagation();startDownloadUI(dlBtn,m.id,fa.senderId);};
    overlay.appendChild(dlBtn);
    const nameEl=document.createElement('div');
    nameEl.style.cssText='color:#fff;font-size:11px;text-shadow:0 1px 3px rgba(0,0,0,.8);max-width:90%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:center;';
    nameEl.textContent=fa.name;
    overlay.appendChild(nameEl);
    const sizeEl=document.createElement('div');
    sizeEl.style.cssText='color:rgba(255,255,255,.75);font-size:10px;';
    sizeEl.textContent=formatSize(fa.size);
    overlay.appendChild(sizeEl);
    wrap.appendChild(overlay);
    return wrap;
  }

  // ── Вспомогательная: строит нижнюю строку (размер · [тип] · spacer · время) ──
  function makeBottomRow(sizeText, extText){
    const row = document.createElement('div');
    row.className = 'tg-file-bottom';
    const sz = document.createElement('span');
    sz.className = 'tg-file-size';
    sz.textContent = sizeText;
    row.appendChild(sz);
    if(extText){
      const badge = document.createElement('span');
      badge.className = 'tg-file-type-badge';
      badge.textContent = extText;
      row.appendChild(badge);
    }
    const spacer = document.createElement('span');
    spacer.className = 'tg-file-spacer';
    row.appendChild(spacer);
    row.appendChild(buildTgFileMeta(m, isSent));
    return row;
  }

  if(isAudio){
    // Аудио — плитка с кнопкой скачать
    const wrap = document.createElement('div');
    wrap.className = 'tg-audio-pending-wrap';
    const dlBtn = document.createElement('div');
    dlBtn.className = 'tg-audio-dl-btn';
    dlBtn.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 15V3\" /> <path d=\"M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4\" /> <path d=\"m7 10 5 5 5-5\" /> </svg>';
    dlBtn.onclick = e => { e.stopPropagation(); startDownloadUI(dlBtn, m.id, fa.senderId); };
    wrap.appendChild(dlBtn);
    const info = document.createElement('div');
    info.className = 'tg-file-info';
    const name = document.createElement('div');
    name.className = 'tg-file-name';
    name.textContent = fa.name ? fa.name.replace(/\.[^/.]+$/,'') : 'Аудио';
    info.appendChild(name);
    info.appendChild(makeBottomRow(formatSize(fa.size) + ' · MP3', ''));
    wrap.appendChild(info);
    return wrap;
  }

  if(isVideo){
    // Видео — плитка с кнопкой скачать
    const wrap = document.createElement('div');
    wrap.className = 'tg-file-wrap';
    const icon = document.createElement('div');
    icon.className = 'tg-file-icon tg-file-icon-dl';
    icon.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 15V3\" /> <path d=\"M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4\" /> <path d=\"m7 10 5 5 5-5\" /> </svg>';
    icon.onclick = e => { e.stopPropagation(); startDownloadUI(icon, m.id, fa.senderId); };
    wrap.appendChild(icon);
    const info = document.createElement('div');
    info.className = 'tg-file-info';
    const name = document.createElement('div');
    name.className = 'tg-file-name';
    name.textContent = fa.name || 'Видео';
    info.appendChild(name);
    const ext = (fa.name||'').split('.').pop().toUpperCase().slice(0,6);
    info.appendChild(makeBottomRow(formatSize(fa.size), ext));
    wrap.appendChild(info);
    return wrap;
  }

  // Обычный файл
  const wrap = document.createElement('div');
  wrap.className = 'tg-file-wrap';
  const icon = document.createElement('div');
  icon.className = 'tg-file-icon tg-file-icon-dl';
  icon.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 15V3\" /> <path d=\"M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4\" /> <path d=\"m7 10 5 5 5-5\" /> </svg>';
  icon.onclick = e => { e.stopPropagation(); startDownloadUI(icon, m.id, fa.senderId); };
  wrap.appendChild(icon);
  const info = document.createElement('div');
  info.className = 'tg-file-info';
  const name = document.createElement('div');
  name.className = 'tg-file-name';
  name.textContent = fa.name;
  info.appendChild(name);
  const ext = (fa.name||'').split('.').pop().toUpperCase().slice(0,6);
  info.appendChild(makeBottomRow(formatSize(fa.size), ext));
  wrap.appendChild(info);
  return wrap;
}


// ── Вспомогательная функция для свайпа (Telegram-стиль) ──
function initSwipeToReply(outer, row, msgId) {
  let startX = 0, currentX = 0, isSwiping = false;
  const threshold = 60; 
  const maxSwipe = 80;  

  const indicator = document.createElement('div');
  indicator.className = 'reply-swipe-indicator';
  indicator.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M20 20v-7a4 4 0 0 0-4-4H4\" /> <path d=\"M9 14 4 9l5-5\" /> </svg>';
  row.appendChild(indicator);

  outer.addEventListener('touchstart', e => {
    if (selectionMode || e.touches.length > 1) return;
    startX = e.touches[0].clientX;
    currentX = startX;
    isSwiping = false;
    row.classList.remove('swiping');
    indicator.classList.remove('visible', 'active');
  }, { passive: true });

  outer.addEventListener('touchmove', e => {
    if (selectionMode || e.touches.length > 1) return;
    currentX = e.touches[0].clientX;
    const diff = startX - currentX;
    if (diff > 10) { 
      isSwiping = true;
      row.classList.add('swiping');
      const move = Math.min(diff, maxSwipe);
      row.style.transform = 'translateX(-' + move + 'px)';
      indicator.classList.add('visible');
      indicator.classList.toggle('active', move >= threshold);
    }
  }, { passive: true });

  outer.addEventListener('touchend', () => {
    if (!isSwiping) return;
    const diff = startX - currentX;
    if (diff >= threshold) {
      _vib('tick');
      startReply(msgId);
    }
    row.classList.remove('swiping');
    row.style.transform = '';
    indicator.classList.remove('visible', 'active');
    isSwiping = false;
  }, { passive: true });
}

function buildRow(m){
  // Системные сообщения (автоудаление и др.) — центрированные, не пузырь
  if(m.type==='system'){
    const el=buildSysMsg(m.text||'');
    el.dataset.msgid=m.id;
    return el;
  }
  const isSent=m.type==='sent';
  // Внешний враппер: полная ширина, flex-row, кружок слева + msg-row справа
  const outer=document.createElement('div');outer.className='msg-row-outer';
  const slot=document.createElement('div');slot.className='msg-check-slot';
  const check=document.createElement('div');check.className='msg-check';check.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M20 6 9 17l-5-5\" /> </svg>';
  slot.appendChild(check);outer.appendChild(slot);
  const row=document.createElement('div');row.className=`msg-row ${m.type}`;row.dataset.msgid=m.id;
  // Помечаем закреплённое сообщение
  if(activePid&&getPinnedMsgs(activePid).some(p=>p.id===m.id)){row.classList.add('pinned-msg');
    // Inject pin badge SVG
    const _bubble = row.querySelector('.bubble');
    if(_bubble && !_bubble.querySelector('.pinned-pin-badge')){
      const _badge = document.createElement('span');
      _badge.className = 'pinned-pin-badge';
      _badge.innerHTML = `<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M12 17v5" /> <path d="M9 10.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H8a2 2 0 0 0 0 4 1 1 0 0 1 1 1z" /> </svg>`;
      _bubble.insertBefore(_badge, _bubble.firstChild);
    }}
  outer.appendChild(row);
  const bubble=document.createElement('div');bubble.className='bubble';
  if(m.forwarded){const fl=document.createElement('span');fl.className='forwarded-label';fl.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"m15 17 5-5-5-5\" /> <path d=\"M4 18v-2a4 4 0 0 1 4-4h12\" /> </svg> переслано';bubble.appendChild(fl);}
  if(m.replyTo){const rd=document.createElement('div');rd.className='reply-preview-inline';rd.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M20 20v-7a4 4 0 0 0-4-4H4\" /> <path d=\"M9 14 4 9l5-5\" /> </svg> '+(m.replyTo.text||'').slice(0,50);rd.onclick=async e=>{
    e.stopPropagation();
    // Закрываем панель стикеров
    const sp=$('stickerPanel');
    if(sp&&sp.classList.contains('open')) await closeStickerPanel();
    const targetRow = document.querySelector(`[data-msgid="${m.replyTo.id}"]`);
    if(!targetRow) return;
    targetRow.scrollIntoView({behavior:'smooth', block:'center'});
    setTimeout(()=>_doMsgRowHighlight(targetRow), 300);
  };bubble.appendChild(rd);}

  if(m.fileAvailable){
    // Получатель — плитка Telegram-стиль без пузыря
    bubble.style.cssText='padding:0;background:transparent;border:none;box-shadow:none;overflow:visible;';
    bubble.appendChild(buildFileAvailableCard(m));

  }else if(m.uploading&&m.file){
    // ОТПРАВИТЕЛЬ — ИДЁТ ЗАГРУЗКА
    const isImg=m.file.type&&m.file.type.startsWith('image/');
    const isVid=m.file.type&&m.file.type.startsWith('video/');
    if(isImg&&m.file.data){
      bubble.style.cssText='padding:4px 4px 7px 4px;';
      const mediaWrap=document.createElement('div');mediaWrap.className='upload-media-wrap';
      const img=document.createElement('img');img.style.cssText='width:100%;height:auto;display:block;border-radius:10px;opacity:.7;filter:brightness(.55)';
      img.src=m.file.data;mediaWrap.appendChild(img);
      mediaWrap.appendChild(buildMediaUploadSpinner(m.id));
      bubble.appendChild(mediaWrap);
      if(m.text){const tn=document.createElement('span');tn.className='bubble-text';tn.style.cssText='display:block;padding:4px 6px 0;';tn.innerHTML=formatText(m.text);bubble.appendChild(tn);}
      bubble.appendChild(mkMeta(m,isSent));
    }else{
      // Файл загружается — плитка без пузыря (видео, аудио, прочее)
      bubble.style.cssText='padding:0;background:transparent;border:none;box-shadow:none;overflow:visible;';
      bubble.appendChild(buildFileCardUploading(m.file,m.id,m,isSent));
      if(m.text){const tn=document.createElement('span');tn.className='bubble-text';tn.style.cssText='display:block;margin-top:4px;padding:6px 8px;background:rgba(255,255,255,.05);border-radius:12px;font-size:13px';tn.innerHTML=formatText(m.text);bubble.appendChild(tn);}
    }

  }else if(m.videoNote){
    // ── ВИДЕО-КРУЖОК ──
    bubble.style.cssText='padding:0;background:transparent;border:none;box-shadow:none;overflow:visible;';
    bubble.appendChild(buildVideoNoteBubble(m,isSent));

  }else if(m.voice?.data||m.voice?.blobId){
    bubble.style.cssText='padding:0;overflow:visible;background:transparent;border:none';
    const vb=buildVoiceBubble(m,isSent);
    const vcont=document.createElement('div');
    vcont.className='voice-bubble-cont';
    const sentBg=getComputedStyle(document.documentElement).getPropertyValue('--bubble-sent-bg').trim()||'linear-gradient(135deg,rgba(0,255,136,.45),rgba(0,255,136,.28))';
    const sentBorder=getComputedStyle(document.documentElement).getPropertyValue('--bubble-sent-border').trim()||'rgba(0,255,136,0.28)';
    vcont.style.cssText+= `;border-radius:18px;overflow:hidden;border:1px solid ${isSent?sentBorder:'rgba(255,255,255,.1)'};background:${isSent?sentBg:'rgba(255,255,255,.07)'};${isSent?'border-bottom-right-radius:4px':'border-bottom-left-radius:4px'}`;
    vcont.appendChild(vb);bubble.appendChild(vcont);

  }else if(m.media?.data||m.media?.blobId){
    // ── TELEGRAM-СТИЛЬ: динамический размер + мета поверх фото ──
    const hasCaption = !!(m.caption && m.caption.trim());

    // Внешняя обёртка
    const outerWrap = document.createElement('div');
    outerWrap.className = hasCaption ? 'media-with-caption' : 'media-bubble';

    // Обёртка фото (position:relative для overlay)
    const imgWrap = document.createElement('div');
    imgWrap.style.cssText = 'position:relative;line-height:0;display:block';
    if(hasCaption){
      imgWrap.style.borderRadius = '0';
    }

    const img = document.createElement('img');
    img.className = 'media-img';
    img.style.cssText = 'display:block;width:100%;height:auto;';

    // Telegram: min 120px, max 260px ширина, max 320px высота
    // Ограничиваем через CSS-переменные, пересчитываем по реальным размерам при загрузке
    const MAX_W = Math.min(260, window.innerWidth * 0.72);
    const MAX_H = 320;
    const MIN_W = 120;
    img.style.maxWidth = MAX_W + 'px';
    img.style.maxHeight = MAX_H + 'px';
    img.style.minWidth  = MIN_W + 'px';
    img.style.objectFit = 'cover';

    // После загрузки — вычисляем оптимальный размер
    img.addEventListener('load', function(){
      const nw = img.naturalWidth, nh = img.naturalHeight;
      if(!nw||!nh) return;
      const ratio = nw / nh;
      let w, h;
      if(ratio >= 1){
        // Альбомная / квадратная
        w = Math.min(nw, MAX_W);
        h = w / ratio;
        if(h > MAX_H){ h = MAX_H; w = h * ratio; }
      } else {
        // Портретная
        h = Math.min(nh, MAX_H);
        w = h * ratio;
        if(w > MAX_W){ w = MAX_W; h = w / ratio; }
        if(w < MIN_W){ w = MIN_W; h = w / ratio; }
      }
      w = Math.round(w); h = Math.round(h);
      img.style.width  = w + 'px';
      img.style.height = h + 'px';
      img.style.maxWidth  = '';
      img.style.maxHeight = '';
      img.style.minWidth  = '';
      img.style.objectFit = '';
      outerWrap.style.width = w + 'px';
    });

    // Загрузка src
    const _setImgSrc = (src) => {
      img.src = src;
      img.onclick = e => { e.stopPropagation(); openLightbox(src); };
    };

    if(m.media.data){
      _setImgSrc(m.media.data);
    } else {
      img.src = 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7';
      img.style.background = 'rgba(255,255,255,.07)';
      img.style.width  = '180px';
      img.style.height = '140px';
      const blobId = m.media.blobId, mimeType = m.media.type||'image/jpeg';
      const loadImg = async () => {
        const src = m.media._tempUrl || await (async()=>{
          const buf = await loadBlob(blobId);
          if(!buf){ img.style.opacity='0.3'; return null; }
          return URL.createObjectURL(new Blob([buf],{type:mimeType}));
        })();
        if(src){ img.style.background=''; _setImgSrc(src); }
      };
      if('IntersectionObserver' in window){
        const io = new IntersectionObserver(en=>{if(en[0].isIntersecting){io.disconnect();loadImg();}},{threshold:0.1,rootMargin:'300px'});
        io.observe(img);
      } else { loadImg(); }
    }

    imgWrap.appendChild(img);

    // Мета-overlay поверх фото (время + галочки) — только без подписи
    if(!hasCaption){
      const overlay = document.createElement('div');
      overlay.className = 'media-meta-overlay';
      const tEl = document.createElement('span');
      tEl.className = 'msg-time-inline';
      tEl.textContent = new Date(m.time).toLocaleTimeString('ru',{hour:'2-digit',minute:'2-digit'});
      overlay.appendChild(tEl);
      if(isSent){
        const tk = document.createElement('span');
        tk.className = 'msg-ticks' + (m.delivered ? ' double' : ' single');
        tk.innerHTML = m.delivered ? '<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M18 6 7 17l-5-5" /> <path d="m22 10-7.5 7.5L13 16" /> </svg>' : '<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M20 6 9 17l-5-5" /> </svg>';
        overlay.appendChild(tk);
      }
      imgWrap.appendChild(overlay);
    }

    outerWrap.appendChild(imgWrap);

    // Подпись — отдельная строка снизу
    if(hasCaption){
      const cap = document.createElement('div');
      cap.className = 'media-caption-row bubble-text';
      cap.innerHTML = formatText(m.caption);
      // Мета в конце подписи (обычный float right)
      cap.appendChild(mkMeta(m,isSent));
      outerWrap.appendChild(cap);
    }

    // Bubble без padding для медиа
    bubble.style.padding = '0';
    bubble.style.overflow = 'hidden';
    bubble.appendChild(outerWrap);
    // Если без подписи — мета уже в overlay, не добавляем снизу
    if(hasCaption){
      /* мета уже внутри cap */ void 0;
    }

  }else if(m.file?.data||m.file?.blobId){
    if(m.file.type.startsWith('audio/')){
      // Аудио: плеер без пузыря — сам плеер IS the message
      bubble.style.cssText='padding:0;background:transparent;border:none;box-shadow:none;overflow:visible;';
      bubble.appendChild(buildAudioPlayer(m.file, isSent, mkMeta(m, isSent)));
      if(m.text){
        const tn=document.createElement('span');
        tn.className='bubble-text';
        tn.style.cssText='display:block;margin-top:4px;font-size:13px;padding:2px 4px';
        tn.innerHTML=formatText(m.text);
        bubble.appendChild(tn);
      }
    }else if(m.file.type.startsWith('video/')){
      // Видео — кастомный плеер Telegram-стиль без пузыря
      bubble.style.padding  = '0';
      bubble.style.overflow = 'hidden';
      bubble.style.background = 'transparent';
      bubble.style.border = 'none';
      const vidPlayer = buildVideoPlayer(m.file, isSent, m);
      bubble.appendChild(vidPlayer);
      if(m.text){
        const tn=document.createElement('span');
        tn.className='bubble-text';
        tn.style.cssText='display:block;margin-top:4px;padding:4px 8px;background:rgba(255,255,255,.05);border-radius:12px;font-size:13px';
        tn.innerHTML=formatText(m.text);
        bubble.appendChild(tn);
      }
    } else {
      // Прочие файлы — TG-плитка без пузыря
      bubble.style.cssText='padding:0;background:transparent;border:none;box-shadow:none;overflow:visible;';
      bubble.appendChild(buildFileCardStatic(m.file, m, isSent));
      if(m.text){
        const tn=document.createElement('span');
        tn.className='bubble-text';
        tn.style.cssText='display:block;margin-top:4px;padding:6px 8px;background:rgba(255,255,255,.05);border-radius:12px;font-size:13px';
        tn.innerHTML=formatText(m.text);
        bubble.appendChild(tn);
      }
    }

  }else if(m.sticker){
    bubble.style.cssText='background:transparent!important;border:none!important;padding:0!important;overflow:visible!important;';
    const sw=document.createElement('div');sw.className='sticker-bubble-wrap';renderStickerVisual(sw,m.sticker);bubble.appendChild(sw);
    const mr=document.createElement('div');mr.className='sticker-meta-row';
    const mi=document.createElement('div');mi.className='sticker-meta-inner';
    const mt=mkMeta(m,isSent);mt.style.float='none';mt.style.margin='0';
    mi.appendChild(mt);mr.appendChild(mi);bubble.appendChild(mr);
  }else{
    const tn=document.createElement('span');tn.className='bubble-text';tn.innerHTML=formatText(m.text||'');
    if(m.edited){const ed=document.createElement('span');ed.className='msg-edited';ed.textContent=' ред.';tn.appendChild(ed);}
    bubble.appendChild(tn);bubble.appendChild(mkMeta(m,isSent));
  }

  row.appendChild(bubble);
  reRenderReactions(row,m.reactions||{},m.id);
  initSwipeToReply(outer, row, m.id);
  outer.addEventListener('click',e=>{if(selectionMode){e.stopPropagation();toggleSelectMessage(m.id,outer);}});
  bubble.addEventListener('contextmenu',e=>{e.preventDefault();if(!selectionMode)showCtx(m,bubble,e.clientX,e.clientY);});
  let pt=null;
  bubble.addEventListener('touchstart',e=>{
    if(selectionMode||e.touches.length>1)return;
    const tgt=e.target;
    // Для vid-bubble разрешаем long-press (но не instant click)
    if(tgt.closest('button,audio,.file-icon-download,.upload-overlay'))return;
    pt=setTimeout(()=>{pt=null;showCtx(m,bubble,e.touches[0].clientX,e.touches[0].clientY);},500);
  },{passive:true});
  bubble.addEventListener('touchend',()=>{clearTimeout(pt);pt=null;});
  bubble.addEventListener('touchmove',()=>{clearTimeout(pt);pt=null;});
  row.querySelectorAll('.spoiler').forEach(el=>el.addEventListener('click',e=>{e.stopPropagation();el.classList.toggle('revealed');}));
  return outer;
}



// ══════════════════════════════════════════════════════
// ── LIGHTBOX + PINCH-TO-ZOOM + PAN ──
// ══════════════════════════════════════════════════════
(function initLightbox(){
  const overlay = $('lightboxOverlay');
  const img     = $('lightboxImg');
  const hint    = $('lightboxZoomHint');

  let scale   = 1;
  let tx = 0, ty = 0;       // текущий translate
  let lastTx = 0, lastTy = 0;

  // Pinch state
  let initDist   = 0;
  let initScale  = 1;
  let pinchMidX  = 0, pinchMidY = 0;

  // Pan state
  let isPanning  = false;
  let panStartX  = 0, panStartY = 0;
  let panStartTx = 0, panStartTy = 0;

  // Double-tap state
  let lastTap = 0;

  let hintTimer = null;

  function applyTransform(animated){
    if(animated) img.style.transition = 'transform .25s cubic-bezier(.4,0,.2,1)';
    else img.style.transition = 'transform .05s linear';
    img.style.transform = `translate(${tx}px,${ty}px) scale(${scale})`;
  }

  function clampTranslate(){
    // Не даём уехать за пределы при zoom > 1
    if(scale <= 1){ tx = 0; ty = 0; return; }
    const rect = img.getBoundingClientRect();
    const ow   = img.naturalWidth  || img.offsetWidth;
    const oh   = img.naturalHeight || img.offsetHeight;
    const vw   = window.innerWidth;
    const vh   = window.innerHeight;
    const scaledW = img.offsetWidth  * scale;
    const scaledH = img.offsetHeight * scale;
    const maxTx = Math.max(0, (scaledW - vw)  / 2);
    const maxTy = Math.max(0, (scaledH - vh) / 2);
    tx = Math.max(-maxTx, Math.min(maxTx, tx));
    ty = Math.max(-maxTy, Math.min(maxTy, ty));
  }

  function resetZoom(animated){
    scale = 1; tx = 0; ty = 0;
    applyTransform(animated !== false);
  }

  function getDist(t1, t2){
    const dx = t1.clientX - t2.clientX;
    const dy = t1.clientY - t2.clientY;
    return Math.sqrt(dx*dx + dy*dy);
  }

  function showHint(){
    if(!hint) return;
    hint.classList.remove('hidden');
    clearTimeout(hintTimer);
    hintTimer = setTimeout(()=> hint.classList.add('hidden'), 2500);
  }

  // ── Touch events ──
  overlay.addEventListener('touchstart', e => {
    if(e.touches.length === 2){
      e.preventDefault();
      isPanning = false;
      initDist  = getDist(e.touches[0], e.touches[1]);
      initScale = scale;
      pinchMidX = (e.touches[0].clientX + e.touches[1].clientX) / 2;
      pinchMidY = (e.touches[0].clientY + e.touches[1].clientY) / 2;
    } else if(e.touches.length === 1){
      // Double-tap detection
      const now = Date.now();
      if(now - lastTap < 300){
        e.preventDefault();
        if(scale > 1){ resetZoom(true); }
        else { scale = 2.5; tx = 0; ty = 0; applyTransform(true); }
        lastTap = 0;
        return;
      }
      lastTap = now;
      // Pan start
      if(scale > 1){
        isPanning  = true;
        panStartX  = e.touches[0].clientX;
        panStartY  = e.touches[0].clientY;
        panStartTx = tx;
        panStartTy = ty;
      }
    }
  }, {passive:false});

  overlay.addEventListener('touchmove', e => {
    if(e.touches.length === 2){
      e.preventDefault();
      const newDist = getDist(e.touches[0], e.touches[1]);
      const ratio   = newDist / initDist;
      scale = Math.min(5, Math.max(1, initScale * ratio));
      applyTransform(false);
    } else if(e.touches.length === 1 && isPanning){
      e.preventDefault();
      tx = panStartTx + (e.touches[0].clientX - panStartX);
      ty = panStartTy + (e.touches[0].clientY - panStartY);
      clampTranslate();
      applyTransform(false);
    }
  }, {passive:false});

  overlay.addEventListener('touchend', e => {
    if(e.touches.length < 2 && initDist > 0){
      // Pinch закончен — фиксируем и клэмпим
      clampTranslate();
      if(scale <= 1.05) resetZoom(true);
      else applyTransform(true);
      initDist = 0;
    }
    if(e.touches.length === 0){
      isPanning = false;
      clampTranslate();
    }
  }, {passive:false});

  // ── Tap на overlay (не на img) — закрыть если scale=1 ──
  overlay.addEventListener('click', e => {
    if(e.target === overlay && scale <= 1) closeLightbox();
  });

  // ── Mouse wheel zoom (десктоп) ──
  overlay.addEventListener('wheel', e => {
    e.preventDefault();
    const delta = e.deltaY > 0 ? 0.85 : 1.15;
    scale = Math.min(5, Math.max(1, scale * delta));
    clampTranslate();
    applyTransform(false);
  }, {passive:false});

  window.openLightbox = src => {
    img.src = src;
    resetZoom(false);
    overlay.classList.add('open');
    showHint();
  };

  window.closeLightbox = () => {
    overlay.classList.remove('open');
    // Небольшая задержка перед сбросом чтобы анимация закрытия прошла
    setTimeout(() => resetZoom(false), 300);
  };
})();

// ── SELECTION ──
let selectionMode=false,selectedMessages=new Set();
function toggleSelectMessage(msgId,outerEl){
  const c=outerEl.querySelector('.msg-check');
  if(selectedMessages.has(msgId)){
    selectedMessages.delete(msgId);
    if(c)c.classList.remove('checked');
    outerEl.classList.remove('row-selected');
  }else{
    selectedMessages.add(msgId);
    if(c)c.classList.add('checked');
    outerEl.classList.add('row-selected');
  }
  updateSelectionBar();
}
function updateSelectionBar(){const count=selectedMessages.size;$('selectionCount').textContent=count+' выбрано';$('selectionBar').classList.toggle('visible',count>0);if(count===0)exitSelectionMode();}
function enterSelectionMode(initialMsgId){
  selectionMode=true;
  selectedMessages.clear();
  if(initialMsgId)selectedMessages.add(initialMsgId);
  document.querySelectorAll('.msg-row-outer').forEach(outer=>{
    const row=outer.querySelector('.msg-row');
    if(!row)return;
    const mid=row.dataset.msgid;
    const isSelected=selectedMessages.has(mid);
    const c=outer.querySelector('.msg-check');
    if(c)c.classList.toggle('checked',isSelected);
    outer.classList.toggle('row-selected',isSelected);
  });
  $('messagesArea').classList.add('selection-mode');
  updateSelectionBar();
}
function exitSelectionMode(){
  selectionMode=false;
  selectedMessages.clear();
  document.querySelectorAll('.msg-row-outer').forEach(outer=>{
    outer.querySelector('.msg-check')?.classList.remove('checked');
    outer.classList.remove('row-selected');
  });
  const area=$('messagesArea');
  area.classList.remove('selection-mode');
  $('selectionBar').classList.remove('visible');
}
function deleteSelectedMessages(){
  if(!selectedMessages.size)return;
  const count=selectedMessages.size;
  showConfirm({
    icon:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg>',
    title:'Удалить сообщения?',
    text:`Будет удалено ${count} ${_pluralMsgWord(count)}. Это действие нельзя отменить.`,
    yesLabel:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg> Удалить',
    noLabel:'Отмена',
    onYes: async ()=>{
      const pidForLock=activePid;
      let newMsgs=null;
      await withChatLock(pidForLock,async()=>{
        const msgs=await loadMsgs(pidForLock);
        // Удаляем blobs выбранных сообщений
        for(const m of msgs){
          if(selectedMessages.has(m.id)){
            if(m.media?.blobId)deleteBlob(m.media.blobId);
            if(m.voice?.blobId)deleteBlob(m.voice.blobId);
            if(m.file?.blobId)deleteBlob(m.file.blobId);
          }
        }
        newMsgs=msgs.filter(m=>!selectedMessages.has(m.id));
        await saveMsgs(pidForLock,newMsgs);
      });
      for(const id of selectedMessages){const row=document.querySelector(`.msg-row[data-msgid="${id}"]`);if(row)(row.closest('.msg-row-outer')||row).remove();if(activePid!==MY_ID){const key=keyCache[activePid];if(key){const enc=await encData(ENC.encode(JSON.stringify({type:'delete',msgId:id,ts:Date.now()})),key);wsSend({type:'send-msg',target:activePid,msgId:uid(),payload:payloadToB64(enc)});}}}
      const last=newMsgs[newMsgs.length-1];await updateChat(activePid,{lastMsg:last?.text||(last?.voice?'Голосовое':last?.media?'Фото':last?.file?`${last.file.name}`:''),lastMsgTime:last?.time?new Date(last.time).getTime():null});exitSelectionMode();toast('Сообщения удалены');
    }
  });
}
window.deleteSelectedMessages=deleteSelectedMessages;window.exitSelectionMode=exitSelectionMode;

// ── HISTORY ──
async function loadChatHistory(pid,reset=false){
  const area=$('messagesArea');const msgs=await loadMsgs(pid);const total=msgs.length;
  const INITIAL_BATCH=50;
  if(reset){
    // Очищаем область сообщений (floatingDate теперь снаружи — не трогаем)
    area.innerHTML='';
    renderedCount=0;
    if(!total){
      const ec=document.createElement('div');ec.className='empty-chat';ec.innerHTML=`<div class="empty-chat-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M2.992 16.342a2 2 0 0 1 .094 1.167l-1.065 3.29a1 1 0 0 0 1.236 1.168l3.413-.998a2 2 0 0 1 1.099.092 10 10 0 1 0-4.777-4.719" /> </svg></div><div class="empty-chat-text">Нет сообщений<br><span style="font-size:11px;font-family:'JetBrains Mono',monospace;opacity:.6">Напишите первое сообщение</span></div>`;area.appendChild(ec);
    }else{
      // Рендерим последние INITIAL_BATCH сообщений сразу
      const frag=document.createDocumentFragment();
      let prevTs=null;
      const startIdx=Math.max(0,msgs.length-INITIAL_BATCH);
      for(let i=startIdx;i<msgs.length;i++){
        const m=msgs[i];
        const ts=new Date(m.time).getTime();
        if(prevTs===null||!isSameDay(prevTs,ts)){
          frag.appendChild(buildDateSeparator(ts));
        }
        frag.appendChild(buildRow(m));
        prevTs=ts;
      }
      area.appendChild(frag);
      renderedCount=total;
      // Предпендим оставшиеся старые сообщения
      if(startIdx>0){
        const scrollHeightBefore=area.scrollHeight;
        const prepFrag=document.createDocumentFragment();
        let prevTs2=null;
        for(let i=0;i<startIdx;i++){
          const m=msgs[i];
          const ts=new Date(m.time).getTime();
          if(prevTs2===null||!isSameDay(prevTs2,ts)){
            prepFrag.appendChild(buildDateSeparator(ts));
          }
          prepFrag.appendChild(buildRow(m));
          prevTs2=ts;
        }
        area.insertBefore(prepFrag,area.firstChild);
        area.scrollTop=area.scrollHeight-scrollHeightBefore;
      }
    }
    area.scrollTop=area.scrollHeight;
    initFloatingDate();
  }else{
    // Инкрементальная загрузка: добавляем только те сообщения которых нет в DOM
    const area=$('messagesArea');
    const wasBot=area.scrollHeight-area.scrollTop-area.clientHeight<80;
    // Собираем набор уже отрисованных ID — надёжнее чем slice по индексу
    const renderedIds=new Set([...area.querySelectorAll('.msg-row[data-msgid],.sys-msg[data-msgid]')].map(r=>r.dataset.msgid));
    const newMsgs=msgs.filter(m=>!renderedIds.has(m.id));
    if(newMsgs.length){
      const frag=document.createDocumentFragment();
      // Для определения разделителя дат берём timestamp последнего сепаратора
      const prevSeps=area.querySelectorAll('.date-separator');
      const lastSep=prevSeps.length?prevSeps[prevSeps.length-1]:null;
      let prevTs=lastSep?parseInt(lastSep.dataset.dateTs):null;
      for(const m of newMsgs){
        const ts=new Date(m.time).getTime();
        if(prevTs===null||!isSameDay(prevTs,ts)){
          const sep=buildDateSeparator(ts);
          frag.appendChild(sep);
          observeDateSeparator(sep);
        }
        frag.appendChild(buildRow(m));
        prevTs=ts;
      }
      area.appendChild(frag);
      renderedCount=total;
      if(wasBot)area.scrollTop=area.scrollHeight;
    }
  }
  updateScrollBtn();syncDeliveredStatuses();
}
function appendOrReloadMsg(pid,m){
  if(activePid!==pid)return;
  const area=$('messagesArea');
  const ec=area.querySelector('.empty-chat');if(ec)ec.remove();
  // ── Дедупликация DOM: если строка уже есть — заменяем, не добавляем новую ──
  const existingRow=area.querySelector(`.msg-row[data-msgid="${m.id}"],.sys-msg[data-msgid="${m.id}"]`);
  if(existingRow){
    existingRow.replaceWith(buildRow(m));
    return;
  }
  const wasBot=area.scrollHeight-area.scrollTop-area.clientHeight<80;
  // Проверяем нужен ли разделитель дат
  const allRows=area.querySelectorAll('.msg-row[data-msgid],.sys-msg[data-msgid]');
  const lastRow=allRows.length?allRows[allRows.length-1]:null;
  if(lastRow){
    const prevSep=area.querySelectorAll('.date-separator');
    const lastSep=prevSep.length?prevSep[prevSep.length-1]:null;
    const lastSepTs=lastSep?parseInt(lastSep.dataset.dateTs):null;
    const newTs=new Date(m.time).getTime();
    if(lastSepTs===null||!isSameDay(lastSepTs,newTs)){
      const sep=buildDateSeparator(newTs);
      area.appendChild(sep);
      observeDateSeparator(sep);
    }
  }
  area.appendChild(buildRow(m));
  // Синхронизируем renderedCount чтобы loadChatHistory(false) не дублировал
  renderedCount=area.querySelectorAll('.msg-row[data-msgid],.sys-msg[data-msgid]').length;
  if(wasBot)area.scrollTop=area.scrollHeight;
  else{const badge=$('scrollBadge');if(badge){badge.textContent=(parseInt(badge.textContent)||0)+1;$('scrollDownBtn').classList.add('has-badge');}}
  updateScrollBtn();
}
function updateScrollBtn(){const area=$('messagesArea'),btn=$('scrollDownBtn');if(!area||!btn)return;const d=area.scrollHeight-area.scrollTop-area.clientHeight;btn.classList.toggle('visible',d>100);if(d<=100){const badge=$('scrollBadge');if(badge)badge.textContent='';btn.classList.remove('has-badge');}}
window.scrollToBottom=()=>{const area=$('messagesArea');area.scrollTo({top:area.scrollHeight,behavior:'smooth'});const badge=$('scrollBadge');if(badge)badge.textContent='';$('scrollDownBtn').classList.remove('has-badge');};
_rebuildScrollHandler();
new ResizeObserver(()=>updateScrollBtn()).observe($('messagesArea'));

// ── CONTEXT MENU ──
let _ctx=null,_emo=null,_back=null;
function rmBack(){if(_back){_back.remove();_back=null;}}
function closeCtx(){if(_ctx){_ctx.remove();_ctx=null;}}
function closeEmo(){if(_emo){_emo.remove();_emo=null;}}
function closeAll(){closeCtx();closeEmo();rmBack();}
function mkBack(){rmBack();_back=document.createElement('div');_back.className='popup-backdrop';_back.addEventListener('click',e=>{e.stopPropagation();closeAll();});document.body.appendChild(_back);}
function _positionPopup(el, anchor, preferredW, preferredH){
  // Универсальное позиционирование попапа относительно anchor-элемента
  // Всегда остаётся внутри viewport по всем четырём сторонам
  const r = anchor.getBoundingClientRect();
  const vw = window.innerWidth;
  const vh = window.innerHeight;
  // Math.ceil — защита от дробных пикселей на Retina
  const pw = Math.ceil(el.offsetWidth)  || preferredW || 220;
  const ph = Math.ceil(el.offsetHeight) || preferredH || 54;
  const GAP = 10; // отступ от края экрана

  // Горизонталь: центрируем по bubble, строго не выходим за экран
  let l = r.left + r.width / 2 - pw / 2;
  l = Math.max(GAP, Math.min(l, vw - pw - GAP));

  // Вертикаль: предпочитаем показывать ВЫШЕ anchor
  let t = r.top - ph - GAP;
  if(t < GAP){
    // Не влезает сверху — показываем снизу
    t = r.bottom + GAP;
  }
  // Если и снизу не влезает — прижимаем к нижнему краю
  if(t + ph > vh - GAP){
    t = Math.max(GAP, vh - ph - GAP);
  }

  el.style.left = l + 'px';
  el.style.top  = t + 'px';
}

function showEmo(msgId,anchor){
  closeEmo();
  if(!_back) mkBack();
  const p = document.createElement('div');
  p.className = 'emoji-picker';
  // Рендерим невидимо чтобы получить точный offsetWidth
  p.style.visibility = 'hidden';
  p.style.top = '-9999px';
  p.style.left = '-9999px';
  QUICK_EMOJIS.forEach(em=>{
    const b = document.createElement('div');
    b.className = 'emoji-pick-btn';
    b.textContent = em;
    b.addEventListener('click', e=>{
      e.stopPropagation();
      if(em==='➕'){closeAll(); showExtendedEmojiPicker(msgId,anchor);}
      else{toggleReaction(msgId,em); closeAll();}
    });
    p.appendChild(b);
  });
  document.body.appendChild(p);
  _emo = p;
  // Двойной rAF: первый кадр — браузер рассчитывает layout,
  // второй кадр — мы читаем точный offsetWidth и позиционируем
  requestAnimationFrame(()=>{
    requestAnimationFrame(()=>{
      p.style.visibility = '';
      p.style.top = '';
      p.style.left = '';
      _positionPopup(p, anchor, 310, 45);
    });
  });
}
function showExtendedEmojiPicker(msgId,anchor){
  closeEmo();
  if(!_back) mkBack();
  const panel = document.createElement('div');
  panel.className = 'emoji-picker-extended';
  const hdr = document.createElement('div');
  hdr.className = 'emoji-picker-header';
  const cb = document.createElement('span');
  cb.className = 'emoji-picker-close';
  cb.innerHTML = '<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M18 6 6 18" /> <path d="m6 6 12 12" /> </svg>';
  cb.addEventListener('click', e=>{e.stopPropagation(); closeAll();});
  hdr.appendChild(cb);
  panel.appendChild(hdr);
  const grid = document.createElement('div');
  grid.className = 'emoji-grid';
  EXTENDED_EMOJIS.slice(0,56).forEach(em=>{
    const btn = document.createElement('div');
    btn.className = 'emoji-grid-btn';
    btn.textContent = em;
    btn.addEventListener('click', e=>{e.stopPropagation(); toggleReaction(msgId,em); closeAll();});
    grid.appendChild(btn);
  });
  panel.appendChild(grid);
  document.body.appendChild(panel);
  _emo = panel;
  // Двойной rAF для точного замера высоты (grid может быть разной высоты)
  panel.style.visibility = 'hidden';
  panel.style.top = '-9999px';
  panel.style.left = '-9999px';
  requestAnimationFrame(()=>{
    requestAnimationFrame(()=>{
      panel.style.visibility = '';
      panel.style.top = '';
      panel.style.left = '';
      _positionPopup(panel, anchor, 300, 320);
    });
  });
}
function showCtx(m,anchor,tx,ty){
  if(m.uploading)return;
  _vib('impactMedium'); // тактильный отклик — открыто контекстное меню (long-press)
  closeAll();mkBack();const menu=document.createElement('div');menu.className='ctx-menu';
  const add=(ic,lb,fn,danger=false)=>{const el=document.createElement('div');el.className='ctx-item'+(danger?' danger':'');el.innerHTML=`<span>${ic}</span><span>${lb}</span>`;el.addEventListener('click',e=>{e.stopPropagation();fn();});menu.appendChild(el);};
  add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M8 14s1.5 2 4 2 4-2 4-2\" /> <line x1=\"9\" x2=\"9.01\" y1=\"9\" y2=\"9\" /> <line x1=\"15\" x2=\"15.01\" y1=\"9\" y2=\"9\" /> </svg>','Реакция',()=>{closeCtx();showEmo(m.id,anchor);});
  add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M20 20v-7a4 4 0 0 0-4-4H4\" /> <path d=\"M9 14 4 9l5-5\" /> </svg>','Ответить',()=>{closeAll();startReply(m.id);});
  // ── ЗАКРЕПИТЬ / ОТКРЕПИТЬ ──
  const pinned=getPinnedMsgs(activePid);
  const isAlreadyPinned=pinned.some(p=>p.id===m.id);
  if(isAlreadyPinned){
    add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 17v5\" /> <path d=\"M15 9.34V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H7.89\" /> <path d=\"m2 2 20 20\" /> <path d=\"M9 9v1.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h11\" /> </svg>','Открепить',()=>{closeAll();showConfirm({icon:'📍',title:'Открепить сообщение?',text:'Сообщение будет откреплено у всех участников чата.',yesLabel:'📍 Открепить',noLabel:'Отмена',onYes:()=>unpinMessage(m.id)});});
  }else{
    add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 17v5\" /> <path d=\"M9 10.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H8a2 2 0 0 0 0 4 1 1 0 0 1 1 1z\" /> </svg>','Закрепить',()=>{closeAll();pinMessage(m);});
  }
  if(m.voice){
    add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M15.2 3a2 2 0 0 1 1.4.6l3.8 3.8a2 2 0 0 1 .6 1.4V19a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2z\" /> <path d=\"M17 21v-7a1 1 0 0 0-1-1H8a1 1 0 0 0-1 1v7\" /> <path d=\"M7 3v4a1 1 0 0 0 1 1h7\" /> </svg>','Сохранить в файлы',async()=>{
      closeAll();
      const fname='voice_'+Date.now()+'.webm';
      if(m.voice.data){await saveToFiles(m.voice.data,'audio/webm',fname,false);}
      else if(m.voice.blobId){await saveToFiles(m.voice.blobId,'audio/webm',fname,true);}
      else toast('Голосовое недоступно','err');
    });
  }
  if(m.videoNote){
    // Видео-кружок — сохраняем как видео файл (аналогично m.media видео)
    const vnExt=(m.videoMime||'video/webm').split('/')[1]?.split(';')[0]||'webm';
    const vnFname='video_note_'+Date.now()+'.'+vnExt;
    const vnMime=m.videoMime||'video/webm';
    add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M15.2 3a2 2 0 0 1 1.4.6l3.8 3.8a2 2 0 0 1 .6 1.4V19a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2z\" /> <path d=\"M17 21v-7a1 1 0 0 0-1-1H8a1 1 0 0 0-1 1v7\" /> <path d=\"M7 3v4a1 1 0 0 0 1 1h7\" /> </svg>','Сохранить видео',async()=>{
      closeAll();
      if(m.videoBlobId){
        await saveToGallery(m.videoBlobId,vnMime,vnFname,true);
      }else if(m.videoData){
        await saveToGallery(m.videoData,vnMime,vnFname,false);
      }else{
        toast('Видео-кружок недоступен','err');
      }
    });
  }
  if(m.media){
    const isVid=m.media.type&&m.media.type.startsWith('video/');
    const ext=m.media.type?(m.media.type.split('/')[1]||'jpg'):'jpg';
    const fname=(isVid?'video_':'photo_')+Date.now()+'.'+ext;
    add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M15.2 3a2 2 0 0 1 1.4.6l3.8 3.8a2 2 0 0 1 .6 1.4V19a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2z\" /> <path d=\"M17 21v-7a1 1 0 0 0-1-1H8a1 1 0 0 0-1 1v7\" /> <path d=\"M7 3v4a1 1 0 0 0 1 1h7\" /> </svg>',isVid?'Сохранить видео':'Сохранить в галерею',async()=>{
      closeAll();
      if(m.media.data){await saveToGallery(m.media.data,m.media.type||'image/jpeg',fname,false);}
      else if(m.media.blobId){await saveToGallery(m.media.blobId,m.media.type||'image/jpeg',fname,true);}
      else toast('Медиа недоступно','err');
    });
  }
  if(m.file?.data||m.file?.blobId){
    const isAudio=m.file.type?.startsWith('audio/');
    const label=isAudio?'Сохранить аудио':'Сохранить в файлы';
    add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M15.2 3a2 2 0 0 1 1.4.6l3.8 3.8a2 2 0 0 1 .6 1.4V19a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2z\" /> <path d=\"M17 21v-7a1 1 0 0 0-1-1H8a1 1 0 0 0-1 1v7\" /> <path d=\"M7 3v4a1 1 0 0 0 1 1h7\" /> </svg>',label,async()=>{
      closeAll();
      if(m.file.data){await saveToFiles(m.file.data,m.file.type,m.file.name,false);}
      else if(m.file.blobId){await saveToFiles(m.file.blobId,m.file.type,m.file.name,true);}
      else toast('Файл недоступен','err');
    });
  }
  if(m.text)add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <rect width=\"8\" height=\"4\" x=\"8\" y=\"2\" rx=\"1\" ry=\"1\" /> <path d=\"M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2\" /> </svg>','Копировать',()=>{closeAll();copyToClipboard(m.text).then(()=>toast('Скопировано ✓'));});
  add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M21.801 10A10 10 0 1 1 17 3.335\" /> <path d=\"m9 11 3 3L22 4\" /> </svg>','Выбрать',()=>{closeAll();enterSelectionMode(m.id);});
  add('<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M11.525 2.295a.53.53 0 0 1 .95 0l2.31 4.679a2.123 2.123 0 0 0 1.595 1.16l5.166.756a.53.53 0 0 1 .294.904l-3.736 3.638a2.123 2.123 0 0 0-.611 1.878l.882 5.14a.53.53 0 0 1-.771.56l-4.618-2.428a2.122 2.122 0 0 0-1.973 0L6.396 21.01a.53.53 0 0 1-.77-.56l.881-5.139a2.122 2.122 0 0 0-.611-1.879L2.16 9.795a.53.53 0 0 1 .294-.906l5.165-.755a2.122 2.122 0 0 0 1.597-1.16z" /> </svg>','В Избранное',()=>{closeAll();forwardToFavorites(m.id);});
  if(m.type==='sent'&&!m.media&&!m.voice&&!m.file&&!m.fileAvailable)add('<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M21.174 6.812a1 1 0 0 0-3.986-3.987L3.842 16.174a2 2 0 0 0-.5.83l-1.321 4.352a.5.5 0 0 0 .623.622l4.353-1.32a2 2 0 0 0 .83-.497z\" /> <path d=\"m15 5 4 4\" /> </svg>','Редактировать',()=>{closeAll();startEdit(m.id);});
  if(m.type==='sent'||activePid===MY_ID){const d=document.createElement('div');d.className='ctx-divider';menu.appendChild(d);add('<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg>','Удалить',()=>{closeAll();deleteMsg(m.id);},true);}
  document.body.appendChild(menu);_ctx=menu;
  requestAnimationFrame(()=>{const mw=menu.offsetWidth||180,mh=menu.offsetHeight||130;const vw=window.innerWidth,vh=window.innerHeight;let l=tx,t=ty;if(l+mw>vw-8)l=vw-mw-8;if(t+mh>vh-8)t=vh-mh-8;if(l<8)l=8;if(t<8)t=8;menu.style.left=l+'px';menu.style.top=t+'px';});
}

// ── MSG SEARCH ──
let srchM=[],srchI=0,srchOn=false;
function resetMsgSearch(){srchOn=false;$('msgSearchBar')?.classList.remove('open');$('searchToggleBtn')?.classList.remove('active');const i=$('msgSearchInput');if(i)i.value='';clrSrch();}
window.toggleMsgSearch=()=>{srchOn=!srchOn;$('msgSearchBar').classList.toggle('open',srchOn);$('searchToggleBtn').classList.toggle('active',srchOn);if(srchOn)setTimeout(()=>$('msgSearchInput').focus(),280);else clrSrch();};
window.closeMsgSearch=()=>{srchOn=false;$('msgSearchBar').classList.remove('open');$('searchToggleBtn').classList.remove('active');const i=$('msgSearchInput');if(i)i.value='';clrSrch();};
function clrSrch(){document.querySelectorAll('.bubble.search-match,.bubble.search-current').forEach(el=>el.classList.remove('search-match','search-current'));srchM=[];srchI=0;const sc=$('searchCount');if(sc)sc.textContent='';}
window.onMsgSearch=()=>{const q=($('msgSearchInput').value||'').trim().toLowerCase();clrSrch();if(!q)return;document.querySelectorAll('.msg-row[data-msgid]').forEach(r=>{const t=r.querySelector('.bubble-text,.media-caption');if(t&&t.textContent.toLowerCase().includes(q)){const b=r.querySelector('.bubble');b.classList.add('search-match');srchM.push(b);}});$('searchCount').textContent=srchM.length?`${srchM.length} совп.`:'Нет';if(srchM.length){srchI=0;hlSrch();}};
function hlSrch(){srchM.forEach(b=>b.classList.remove('search-current'));if(!srchM.length)return;const c=srchM[srchI];c.classList.add('search-current');c.scrollIntoView({behavior:'smooth',block:'center'});$('searchCount').textContent=`${srchI+1} / ${srchM.length}`;}
window.searchNav=d=>{if(!srchM.length)return;srchI=(srchI+d+srchM.length)%srchM.length;hlSrch();};

// ── LEAVE / DELETE ──
// Отправляем last-seen всем контактам у которых есть активные чаты
// (только если включена настройка "Время захода")
async function broadcastLastSeenOff(){
  // Рассылаем last-seen-off — скрываем время захода у всех контактов
  if(!wsUp) return;
  const chats = await loadChats();
  for(const chat of chats){
    if(chat.peerId === MY_ID) continue;
    const key = keyCache[chat.peerId];
    if(!key) continue;
    try{
      const enc = await encData(ENC.encode(JSON.stringify({type:'last-seen-off',v:1})), key);
      wsSend({type:'send-msg', target:chat.peerId, msgId:uid(), payload:payloadToB64(enc)});
    }catch(e){}
  }
}

let _lastSeenBroadcastTs = 0;
async function broadcastLastSeen(){
  if(!lsGet('bc_last_seen_enabled', false)) return;
  if(!wsUp) return;
  const now = Date.now();
  if(now - _lastSeenBroadcastTs < 5000) return;
  _lastSeenBroadcastTs = now;
  const ts = now;
  const chats = await loadChats();
  for(const chat of chats){
    if(chat.peerId === MY_ID) continue;
    const key = keyCache[chat.peerId];
    if(!key) continue;
    try{
      const enc = await encData(ENC.encode(JSON.stringify({type:'last-seen',ts,v:1})), key);
      wsSend({type:'send-msg', target:chat.peerId, msgId:uid(), payload:payloadToB64(enc)});
    }catch(e){}
  }
}

window.leaveChat=async()=>{
  await broadcastLastSeen().catch(()=>{});
  activePid='';stopPeerActivity();_stopConnDots();cancelEdit();cancelReply();resetMsgSearch();exitSelectionMode();closeAll();clearMediaPreview();if(isRecording)cancelRecording();_stopPreviewAudio();$('voicePreviewBar').classList.remove('active');
  // Закрываем видеоплеер если открыт
  if(typeof closeVideoLightbox==='function') window.closeVideoLightbox();
  // Мгновенно скрываем панель стикеров (position:fixed — висит поверх всех экранов)
  const sp=$('stickerPanel');
  if(sp){sp.classList.remove('open','frozen');document.removeEventListener('click',_stickerOutsideClick);}
  const ib=$('chatInputBar');if(ib)ib.classList.remove('sticker-mode','attach-mode');
  window._stickerNavLock=false;
  // Мгновенно скрываем панель вложений
  const asheet=$('attachSheet'),abackdrop=$('attachBackdrop');
  if(asheet){
    const agrid=asheet.querySelector('.attach-grid');
    if(agrid)agrid.classList.remove('attach-items-animate');
    asheet.classList.remove('open','dragging');asheet.style.transform='';
    document.removeEventListener('click',_attachOutsideClick);
  }
  if(abackdrop){abackdrop.classList.remove('open');abackdrop.style.opacity='';}
  // Убираем оверлей подсветки строки если остался
  document.querySelectorAll('.msg-row.row-highlighted').forEach(r=>{
    r.style.removeProperty('--hl-top');
    r.style.removeProperty('--hl-height');
    r.classList.remove('row-highlighted');
  });
  const hlOld=document.querySelector('.msg-row-hl');if(hlOld)hlOld.remove();
  // Скрываем панель закреплённого при выходе из чата
  const pb=$('pinnedBar');if(pb)pb.classList.remove('visible');
  goHome();
};
window.deleteChat=async()=>{
  if(!activePid)return;
  if(activePid===MY_ID){toast('Чат "Избранное" нельзя удалить','warn');return;}
  showConfirm({
    icon:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg>',
    title:'Удалить чат?',
    text:'История сообщений и ключ шифрования будут удалены с этого устройства. Собеседник не будет уведомлён.',
    yesLabel:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M10 11v6" /> <path d="M14 11v6" /> <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /> <path d="M3 6h18" /> <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /> </svg> Удалить чат',
    noLabel:'Отмена',
    onYes: async()=>{
      const pid=activePid;activePid='';stopPeerActivity();cancelEdit();cancelReply();resetMsgSearch();exitSelectionMode();closeAll();
  // Удаляем все blobs сообщений чата
  try{
    const allMsgs=await loadMsgs(pid);
    for(const m of allMsgs){
      if(m.media?.blobId)deleteBlob(m.media.blobId);
      if(m.voice?.blobId)deleteBlob(m.voice.blobId);
      if(m.file?.blobId)deleteBlob(m.file.blobId);
    }
  }catch(e){}
  await clearMsgs(pid);await deleteChatData(pid);delete keyCache[pid];localStorage.removeItem(`bc_pwd_${pid}`);localStorage.removeItem(`bc_secret_${pid}`);sessionStorage.removeItem(`bc_pwd_${pid}`);goHome();toast('Чат удалён');}
  });
};

// ══════════════════════════════════════════════════════
// ── БЛОКИРОВКА КОНТАКТА (Telegram-style) ──
// ══════════════════════════════════════════════════════
function isContactBlocked(pid){
  const blocked=lsGet('bc_blocked_contacts',[]);
  return blocked.includes(pid);
}

function setContactBlocked(pid,block){
  let blocked=lsGet('bc_blocked_contacts',[]);
  if(block){
    if(!blocked.includes(pid))blocked.push(pid);
  }else{
    blocked=blocked.filter(id=>id!==pid);
  }
  lsSet('bc_blocked_contacts',blocked);
}

function updateBlockUI(pid){
  const btn=$('blockContactBtn');
  if(!btn)return;
  const blocked=isContactBlocked(pid);
  const inputBar=$('chatInputBar');
  const banner=$('blockedBanner');
  if(blocked){
    // Кнопка становится ❌ (разблокировать)
    btn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"m15 9-6 6\" /> <path d=\"m9 9 6 6\" /> </svg>';
    btn.classList.add('block-active');
    btn.title='Разблокировать контакт';
    // Статус в шапке
    setPeerStatus('blocked','Контакт заблокирован');
    // Блокируем ввод
    if(inputBar)inputBar.classList.add('blocked-mode');
    if(banner)banner.classList.add('visible');
    // Отключаем кнопку отправки
    $('sendBtn').disabled=true;
  }else{
    // Кнопка становится 🚫 (заблокировать)
    btn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M4.929 4.929 19.07 19.071\" /> </svg>';
    btn.classList.remove('block-active');
    btn.title='Заблокировать контакт';
    // Возвращаем нормальный статус (только если не заблокированы самим собой)
    if(!lsGet(`bc_blocked_by_${pid}`,false)){updateBar();}
    else{setPeerStatus('blocked','Вы заблокированы');}
    // Разблокируем ввод
    if(inputBar)inputBar.classList.remove('blocked-mode');
    if(banner)banner.classList.remove('visible');
    // Восстанавливаем кнопку отправки
    updateSendBtn();
  }
}

async function _sendBlockSignal(pid, isBlocked){
  if(!wsUp||!keyCache[pid])return;
  try{
    const enc=await encData(ENC.encode(JSON.stringify({type:'block-status',blocked:isBlocked,ts:Date.now()})),keyCache[pid]);
    wsSend({type:'send-msg',target:pid,msgId:uid(),payload:payloadToB64(enc),ephemeral:true});
  }catch(e){}
}

window.toggleBlockContact=function(){
  if(!activePid)return;
  if(activePid===MY_ID)return;
  const blocked=isContactBlocked(activePid);
  if(!blocked){
    // Блокируем
    showConfirm({
      icon:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <circle cx="12" cy="12" r="10" /> <path d="M4.929 4.929 19.07 19.071" /> </svg>',
      title:'Заблокировать контакт?',
      text:'Контакт будет заблокирован. Новые сообщения от него перестанут поступать. Вы сможете разблокировать его в любой момент.',
      yesLabel:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"M4.929 4.929 19.07 19.071\" /> </svg> Заблокировать',
      noLabel:'Отмена',
      onYes:()=>{
        setContactBlocked(activePid,true);
        updateBlockUI(activePid);
        _sendBlockSignal(activePid,true).catch(()=>{});
        toast('Контакт заблокирован');
        renderChatList();
      }
    });
  }else{
    // Разблокируем
    showConfirm({
      icon:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <circle cx="12" cy="12" r="10" /> <path d="m15 9-6 6" /> <path d="m9 9 6 6" /> </svg>',
      title:'Разблокировать контакт?',
      text:'Контакт будет разблокирован. Вы снова сможете получать от него сообщения и общаться.',
      yesLabel:'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <circle cx=\"12\" cy=\"12\" r=\"10\" /> <path d=\"m15 9-6 6\" /> <path d=\"m9 9 6 6\" /> </svg> Разблокировать',
      noLabel:'Отмена',
      onYes:()=>{
        setContactBlocked(activePid,false);
        updateBlockUI(activePid);
        _sendBlockSignal(activePid,false).catch(()=>{});
        toast('Контакт разблокирован');
        renderChatList();
      }
    });
  }
};

document.addEventListener('visibilitychange',()=>{
  if(document.visibilityState==='hidden'){lastHiddenTime=Date.now();_lastSeenBroadcastTs=0;broadcastLastSeen().catch(()=>{});}
  else{ensureWS();const hd=Date.now()-lastHiddenTime;if(ignoreNextVisibilityReturn){ignoreNextVisibilityReturn=false;}else if(hd>3000){if(activePid)leaveChat();else goHome();}else if(activePid&&wsUp)wsSend({type:'query-presence',target:activePid});}
});
// Надёжная отправка last-seen при закрытии/сворачивании
window.addEventListener('pagehide', () => { _lastSeenBroadcastTs=0; broadcastLastSeen().catch(()=>{}); });
window.addEventListener('beforeunload', () => { _lastSeenBroadcastTs=0; broadcastLastSeen().catch(()=>{}); });

$('contactsSearch')?.addEventListener('input',renderContacts);
$('homeSearch')?.addEventListener('input',()=>renderChatList());

// ── VOICE RECORDING ──
let mediaRecorder=null,audioChunks=[],isRecording=false;
let recStartTime=0,recTimerInterval=null,recAnimFrame=null;
let audioCtx=null,analyser=null,micStream=null;
let previewBlob=null,previewDur=0,previewWaveform=[];
let previewAudio=null,previewPlaying=false,previewRafId=null;
let pvbBarEls=[];
let _recActivityHb=null; // heartbeat для статуса "записывает"
const REC_WAVE_BARS=20;

function initRecWaveform(){const w=$('recWaveformLive');w.innerHTML='';for(let i=0;i<REC_WAVE_BARS;i++){const b=document.createElement('div');b.className='rec-wave-bar';b.style.height='3px';w.appendChild(b);}}
function animateRecWaveform(){if(!isRecording)return;const bars=$('recWaveformLive').children;if(analyser){const data=new Uint8Array(analyser.frequencyBinCount);analyser.getByteFrequencyData(data);const step=Math.floor(data.length/REC_WAVE_BARS);for(let i=0;i<REC_WAVE_BARS;i++){const h=Math.max(3,Math.round((data[i*step]/255)*20));if(bars[i])bars[i].style.height=h+'px';}}else{for(let i=0;i<bars.length;i++)bars[i].style.height=Math.max(3,Math.round(Math.random()*18))+'px';}recAnimFrame=requestAnimationFrame(animateRecWaveform);}
function updateRecTimer(){const el=Date.now()-recStartTime;const s=Math.floor(el/1000);$('recTimer').textContent=fmtDur(s);const pct=(el/(MAX_VOICE_SEC*1000))*100;const bar=$('recLimitBar');bar.style.width=Math.min(pct,100)+'%';bar.className='rec-limit-bar'+(pct>80?' crit':pct>55?' warn':'');if(el>=MAX_VOICE_SEC*1000)stopRecordingForPreview();}

window.toggleRecording=async function(){
  if(processingFiles)return;if(isRecording){stopRecordingForPreview();return;}
  try{
    // ── СТУДИЙНЫЕ аудио-параметры (Идеально чистый звук Float32 48kHz) ──
    // Отключаем встроенную браузерную обработку, которая может "жевать" звук
    const stream=await navigator.mediaDevices.getUserMedia({audio:{
      channelCount:1,
      sampleRate:48000,
      echoCancellation:false, // Отключаем для максимальной чистоты (если в наушниках) или ставим true только если нужен эхоподав
      noiseSuppression:false, // Отключаем встроенный шумодав браузера (портит тембр)
      autoGainControl:false,  // Отключаем прыжки громкости
      latency:0,
    }});micStream=stream;
    try{
      audioCtx=new(window.AudioContext||window.webkitAudioContext)();
      analyser=audioCtx.createAnalyser();
      analyser.fftSize=64;
      // Применяем аудиофильтры к потоку и обновляем micStream
      micStream = _applyAudioFiltersToStream(stream, audioCtx, analyser);
      // Analyser уже подключен внутри _applyAudioFiltersToStream
    }catch(e){analyser=null;}
    
    // Выбираем формат: Студийный битрейт Opus (192-256kbps)
    let mime='audio/webm;codecs=opus',bitrate=192000;
    if(!MediaRecorder.isTypeSupported(mime)){mime='audio/webm';bitrate=192000;}
    if(!MediaRecorder.isTypeSupported(mime)){mime='audio/ogg;codecs=opus';bitrate=192000;}
    if(!MediaRecorder.isTypeSupported(mime)){mime='';bitrate=192000;}
    mediaRecorder=new MediaRecorder(micStream,mime?{mimeType:mime,audioBitsPerSecond:bitrate}:{audioBitsPerSecond:bitrate});
    audioChunks=[];mediaRecorder.ondataavailable=e=>{if(e.data?.size>0)audioChunks.push(e.data);};
    // timeslice 200ms — оптимальный баланс: не слишком часто (артефакты),
    // не слишком редко (потеря при обрыве). Telegram использует ~200-500ms.
    mediaRecorder.start(200);
    isRecording=true;recStartTime=Date.now();initRecWaveform();
    $('voiceRecordOverlay').classList.add('active');$('micBtn').classList.add('recording');$('micBtn').innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <rect width=\"18\" height=\"18\" x=\"3\" y=\"3\" rx=\"2\" /> </svg>';
    _vib('impactMedium'); // тактильный отклик — началась запись
    recTimerInterval=setInterval(updateRecTimer,200);animateRecWaveform();
    // Сигнал "записывает голосовое" + heartbeat каждые 3с
    _sendActivitySignal(activePid,'recording');
    _recActivityHb=setInterval(()=>{if(activePid&&isRecording)_sendActivitySignal(activePid,'recording');},3000);
  }catch(e){if(e.name==='NotAllowedError')toast('Нет доступа к микрофону','err');else toast('Ошибка микрофона','err');}
};

window.cancelRecording=function(){if(!isRecording)return;clearInterval(_recActivityHb);_recActivityHb=null;_stopRecordingInternal();audioChunks=[];_sendActivityStop(activePid);_vib('notificationWarning');toast('Запись отменена');};

window.stopRecordingForPreview=async function(){
  if(!isRecording)return;const capturedStart=recStartTime;
  clearInterval(_recActivityHb);_recActivityHb=null;
  const chunks=await _stopRecordingInternal();if(!chunks?.length){toast('Пустая запись','warn');return;}
  const durSec=Math.max(1,Math.round((Date.now()-capturedStart)/1000));
  const blob=new Blob(chunks,{type:chunks[0]?.type||'audio/webm'});
  if(!blob.size){toast('Пустая запись','warn');return;}
  previewBlob=blob;previewDur=durSec;previewWaveform=generateWaveform();_showPreviewBar();
  _vib('impactLight'); // тактильный отклик — запись остановлена, готов предпросмотр
  // Сигнал "отправляет голосовое" + heartbeat пока preview открыт
  _sendActivitySignal(activePid,'sending_voice');
  _recActivityHb=setInterval(()=>{if(activePid&&previewBlob)_sendActivitySignal(activePid,'sending_voice');},3000);
};

function _showPreviewBar(){const wfEl=$('pvbWaveform');wfEl.innerHTML='';pvbBarEls=[];for(let i=0;i<PREV_WF_BARS;i++){const b=document.createElement('div');b.className='pvb-bar';const v=previewWaveform[Math.floor(i*previewWaveform.length/PREV_WF_BARS)]||0.3;b.style.height=Math.max(3,Math.round(v*18))+'px';wfEl.appendChild(b);pvbBarEls.push(b);}$('pvbTimer').textContent=fmtDur(previewDur);$('pvbPlayBtn').innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>';$('pvbPlayBtn').classList.remove('playing');$('voicePreviewBar').classList.add('active');}
function _stopPreviewAudio(){previewPlaying=false;cancelAnimationFrame(previewRafId);if(previewAudio){try{previewAudio.pause();}catch(e){}previewAudio=null;}const pb=$('pvbPlayBtn');if(pb){pb.innerHTML='<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z" /> </svg>';pb.classList.remove('playing');}}
function _updatePreviewProgress(){
  if(!previewAudio||previewAudio.paused)return;
  // Используем previewDur (сохранённая длительность записи) — всегда точная,
  // в отличие от audio.duration который может быть NaN/Infinity для blob URL
  const dur=previewDur>0?previewDur:(()=>{const d=previewAudio.duration;return(d&&isFinite(d)&&d>0)?d:0;})();
  const cur=previewAudio.currentTime||0;
  const pct=dur>0?Math.min(cur/dur,1):0;
  $('pvbTimer').textContent=`${fmtDur(Math.floor(cur))} / ${fmtDur(Math.round(dur))}`;
  const count=Math.round(pct*pvbBarEls.length);
  pvbBarEls.forEach((b,i)=>{
    b.classList.toggle('played',i<count);
    b.classList.toggle('active-pvb',i===count-1&&count>0);
  });
  previewRafId=requestAnimationFrame(_updatePreviewProgress);
}
window.togglePreviewPlay=function(){
  if(!previewBlob)return;
  if(previewPlaying){previewAudio.pause();return;}
  if(!previewAudio){
    const url=URL.createObjectURL(previewBlob);
    previewAudio=new Audio(url);
    previewAudio.onended=()=>{
      previewPlaying=false;
      $('pvbPlayBtn').innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>';
      $('pvbPlayBtn').classList.remove('playing');
      cancelAnimationFrame(previewRafId);
      $('pvbTimer').textContent=fmtDur(previewDur);
      pvbBarEls.forEach(b=>{b.classList.remove('played');b.classList.remove('active-pvb');});
    };
    previewAudio.onpause=()=>{
      previewPlaying=false;
      $('pvbPlayBtn').innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>';
      $('pvbPlayBtn').classList.remove('playing');
      cancelAnimationFrame(previewRafId);
    };
    // Когда метаданные загрузятся — обновим прогресс если уже играем
    previewAudio.addEventListener('loadedmetadata',()=>{if(previewPlaying)_updatePreviewProgress();});
  }
  previewAudio.play().then(()=>{
    previewPlaying=true;
    $('pvbPlayBtn').innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <rect x=\"14\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> <rect x=\"5\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> </svg>';
    $('pvbPlayBtn').classList.add('playing');
    // Запускаем сразу — previewDur известен заранее
    cancelAnimationFrame(previewRafId);
    _updatePreviewProgress();
  }).catch(console.error);
};
window.seekPreview=function(e){
  if(!previewAudio)return;
  const dur=previewDur>0?previewDur:(()=>{const d=previewAudio.duration;return(d&&isFinite(d)&&d>0)?d:0;})();
  if(!dur)return;
  const r=$('pvbWaveform').getBoundingClientRect();
  const pct=Math.max(0,Math.min((e.clientX-r.left)/r.width,1));
  previewAudio.currentTime=pct*dur;
  if(!previewPlaying){
    const count=Math.round(pct*pvbBarEls.length);
    pvbBarEls.forEach((b,i)=>{b.classList.toggle('played',i<count);b.classList.toggle('active-pvb',i===count-1&&count>0);});
    $('pvbTimer').textContent=`${fmtDur(Math.floor(previewAudio.currentTime))} / ${fmtDur(Math.round(dur))}`;
  }
};

window.resumeRecording=async function(){
  clearInterval(_recActivityHb);_recActivityHb=null;
  _stopPreviewAudio();$('voicePreviewBar').classList.remove('active');const oldChunks=[...audioChunks];
  try{
    const stream=await navigator.mediaDevices.getUserMedia({audio:{
      channelCount:1,
      sampleRate:48000,
      echoCancellation:false,
      noiseSuppression:false,
      autoGainControl:false,
      latency:0,
    }});micStream=stream;
    try{
      audioCtx=new(window.AudioContext||window.webkitAudioContext)();
      analyser=audioCtx.createAnalyser();analyser.fftSize=64;
      const source=audioCtx.createMediaStreamSource(stream);
      const highPass=audioCtx.createBiquadFilter(); highPass.type='highpass'; highPass.frequency.value=60; highPass.Q.value=0.5;
      const compressor=audioCtx.createDynamicsCompressor(); compressor.threshold.value=-24; compressor.knee.value=30; compressor.ratio.value=3; compressor.attack.value=0.005; compressor.release.value=0.1;
      const gainNode=audioCtx.createGain(); gainNode.gain.value=1.2;
      source.connect(highPass); highPass.connect(compressor); compressor.connect(gainNode); gainNode.connect(analyser);
      const dest=audioCtx.createMediaStreamDestination(); gainNode.connect(dest); micStream=dest.stream;
    }catch(e){analyser=null;}
    let mime='audio/webm;codecs=opus',bitrate=192000;
    if(!MediaRecorder.isTypeSupported(mime)){mime='audio/webm';bitrate=192000;}
    if(!MediaRecorder.isTypeSupported(mime)){mime='audio/ogg;codecs=opus';bitrate=192000;}
    if(!MediaRecorder.isTypeSupported(mime)){mime='';bitrate=192000;}
    mediaRecorder=new MediaRecorder(micStream,mime?{mimeType:mime,audioBitsPerSecond:bitrate}:{audioBitsPerSecond:bitrate});
    audioChunks=oldChunks;mediaRecorder.ondataavailable=e=>{if(e.data?.size>0)audioChunks.push(e.data);};mediaRecorder.start(200);
    isRecording=true;recStartTime=Date.now()-previewDur*1000;initRecWaveform();
    $('voiceRecordOverlay').classList.add('active');$('micBtn').classList.add('recording');$('micBtn').innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <rect width=\"18\" height=\"18\" x=\"3\" y=\"3\" rx=\"2\" /> </svg>';
    _vib('impactMedium'); // тактильный отклик — запись продолжена
    recTimerInterval=setInterval(updateRecTimer,200);animateRecWaveform();previewBlob=null;previewDur=0;previewWaveform=[];
    _sendActivitySignal(activePid,'recording');
    _recActivityHb=setInterval(()=>{if(activePid&&isRecording)_sendActivitySignal(activePid,'recording');},3000);
  }catch(e){toast('Ошибка микрофона','err');$('voicePreviewBar').classList.add('active');}
};

window.cancelPreview=function(){clearInterval(_recActivityHb);_recActivityHb=null;_stopPreviewAudio();previewBlob=null;previewDur=0;previewWaveform=[];pvbBarEls=[];$('voicePreviewBar').classList.remove('active');_sendActivityStop(activePid);_vib('notificationWarning');toast('Запись удалена');};

window.sendPreviewVoice=async function(){
  if(!previewBlob)return;clearInterval(_recActivityHb);_recActivityHb=null;_stopPreviewAudio();const blob=previewBlob,dur=previewDur,wf=previewWaveform;
  previewBlob=null;previewDur=0;previewWaveform=[];pvbBarEls=[];$('voicePreviewBar').classList.remove('active');
  _vib('impactLight'); // тактильный отклик — голосовое отправляется
  const reader=new FileReader();reader.onload=async e=>{await sendVoiceMessage(e.target.result,dur,wf);};reader.readAsDataURL(blob);
};

function _applyAudioFiltersToStream(stream, existingAudioCtx = null, existingAnalyser = null) {
  const audioCtx = existingAudioCtx || new (window.AudioContext || window.webkitAudioContext)();
  const source = audioCtx.createMediaStreamSource(stream);

  // 1. High-pass фильтр (убираем только инфразвук)
  const highPass = audioCtx.createBiquadFilter();
  highPass.type = 'highpass';
  highPass.frequency.value = 20;
  highPass.Q.value = 1.0;

  // 2. High Shelf фильтр для подъема высоких частот (кристальность)
  const highShelf = audioCtx.createBiquadFilter();
  highShelf.type = 'highshelf';
  highShelf.frequency.value = 5000;
  highShelf.gain.value = 6;

  // 3. Очень легкая компрессия для плотности без искажений
  const compressor = audioCtx.createDynamicsCompressor();
  compressor.threshold.value = -12;
  compressor.knee.value = 8;
  compressor.ratio.value = 1.2;
  compressor.attack.value = 0.001;
  compressor.release.value = 0.03;

  // 4. Усиление (Gain)
  const gainNode = audioCtx.createGain();
  gainNode.gain.value = 1.0;

  // Подключаем цепочку
  source.connect(highPass);
  highPass.connect(highShelf);
  highShelf.connect(compressor);
  compressor.connect(gainNode);

  // Если есть анализатор, подключаем его
  if (existingAnalyser) {
    gainNode.connect(existingAnalyser);
  }

  // Создаем MediaStreamDestination для передачи обработанного звука
  const dest = audioCtx.createMediaStreamDestination();
  gainNode.connect(dest);

  return dest.stream;
}

function _stopRecordingInternal(){return new Promise(resolve=>{clearInterval(recTimerInterval);cancelAnimationFrame(recAnimFrame);isRecording=false;$('voiceRecordOverlay').classList.remove('active');$('micBtn').classList.remove('recording');$('micBtn').innerHTML='<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M12 19v3" /> <path d="M19 10v2a7 7 0 0 1-14 0v-2" /> <rect x="9" y="2" width="6" height="13" rx="3" /> </svg>';$('recLimitBar').style.width='0%';$('recLimitBar').className='rec-limit-bar';if(micStream){micStream.getTracks().forEach(t=>t.stop());micStream=null;}if(audioCtx){try{audioCtx.close();}catch(e){}audioCtx=null;analyser=null;}if(mediaRecorder&&mediaRecorder.state!=='inactive'){mediaRecorder.addEventListener('stop',()=>resolve(audioChunks),{once:true});mediaRecorder.stop();}else resolve(audioChunks);});}
function generateWaveform(){const arr=[];for(let i=0;i<WF_BARS;i++)arr.push(Math.max(0.08,Math.min(1,Math.random()*0.9+0.08)));return arr;}

async function sendVoiceMessage(dataURL,duration,waveform){
  if(activePid===MY_ID){
    const msgId=uid(),ts=Date.now();
    // Сохраняем голосовое в blobs
    try{const vbuf=base64ToArrayBuffer(dataURL.replace(/^data:[^,]+,/,''));await saveBlob(msgId+'_voice',vbuf);}catch(e){}
    const m={id:msgId,text:'',type:'sent',time:new Date(ts).toISOString(),reactions:{},edited:false,replyTo:replyTo||null,delivered:true,forwarded:false,voice:{waveform,blobId:msgId+'_voice',data:dataURL},voiceDuration:duration,voiceListened:true};
    await upsertMsg(MY_ID,m);await updateChat(MY_ID,{lastMsg:'Голосовое',lastMsgTime:ts});appendOrReloadMsg(MY_ID,m);cancelReply();return;
  }
  if(!wsUp){toast('Нет связи с сервером','err');return;}
  let key;try{key=await ensureKey(activePid);}catch(e){return;}
  await drainPending(activePid,key);
  const msgId=uid(),ts=Date.now();
  const env={type:'voice',id:msgId,text:'',ts,replyTo:replyTo||null,forwarded:false,voice:{data:dataURL,waveform},voiceDuration:duration};
  try{
    const enc=await encData(ENC.encode(JSON.stringify(env)),key);
    wsSend({type:'send-msg',target:activePid,msgId,payload:payloadToB64(enc)});
    // Сохраняем голосовое отправителя в blobs
    try{const vbuf=base64ToArrayBuffer(dataURL.replace(/^data:[^,]+,/,''));await saveBlob(msgId+'_voice',vbuf);}catch(e){}
    const m={id:msgId,text:'',type:'sent',time:new Date(ts).toISOString(),reactions:{},edited:false,replyTo:replyTo||null,delivered:false,forwarded:false,voice:{waveform:env.voice.waveform,blobId:msgId+'_voice',data:dataURL},voiceDuration:duration,voiceListened:false};
    await upsertMsg(activePid,m);await updateChat(activePid,{lastMsg:'Голосовое',lastMsgTime:ts});appendOrReloadMsg(activePid,m);cancelReply();
    _sendActivityStop(activePid); // сбрасываем статус у получателя
  }catch(e){toast('Ошибка отправки голосового','err');}
}


// ══════════════════════════════════════════════════════════════════════
// ── ВИДЕО-КРУЖКИ (Video Notes) — Telegram-style ──
// ══════════════════════════════════════════════════════════════════════

// ── МИК-КНОПКА: РЕЖИМ + LONG-PRESS ──
let _micMode=lsGet('bc_mic_mode','voice'); // 'voice' | 'video'
let _micLongTimer=null;
let _micLongFired=false;
const MAX_VN_SEC=60;
const VN_CIRCUMFERENCE=2*Math.PI*120; // r=120 → ≈753.98

function _applyMicMode(animate){
  const btn=$('micBtn');
  if(!btn)return;
  const isVideo=_micMode==='video';
  if(animate){
    btn.classList.add('switch-anim');
    setTimeout(()=>{
      btn.innerHTML=isVideo?'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M13.997 4a2 2 0 0 1 1.76 1.05l.486.9A2 2 0 0 0 18.003 7H20a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V9a2 2 0 0 1 2-2h1.997a2 2 0 0 0 1.759-1.048l.489-.904A2 2 0 0 1 10.004 4z\" /> <circle cx=\"12\" cy=\"13\" r=\"3\" /> </svg>':'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 19v3\" /> <path d=\"M19 10v2a7 7 0 0 1-14 0v-2\" /> <rect x=\"9\" y=\"2\" width=\"6\" height=\"13\" rx=\"3\" /> </svg>';
      btn.classList.toggle('mode-video',isVideo);
    },120);
    setTimeout(()=>btn.classList.remove('switch-anim'),300);
  }else{
    btn.innerHTML=isVideo?'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M13.997 4a2 2 0 0 1 1.76 1.05l.486.9A2 2 0 0 0 18.003 7H20a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V9a2 2 0 0 1 2-2h1.997a2 2 0 0 0 1.759-1.048l.489-.904A2 2 0 0 1 10.004 4z\" /> <circle cx=\"12\" cy=\"13\" r=\"3\" /> </svg>':'<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 19v3\" /> <path d=\"M19 10v2a7 7 0 0 1-14 0v-2\" /> <rect x=\"9\" y=\"2\" width=\"6\" height=\"13\" rx=\"3\" /> </svg>';
    btn.classList.toggle('mode-video',isVideo);
  }
}

function _toggleMicMode(){
  if(isRecording||_vnRecording)return;
  _micMode=(_micMode==='voice')?'video':'voice';
  lsSet('bc_mic_mode',_micMode);
  _applyMicMode(true);
  if(typeof _vib==='function')_vib('tick');
}

function _initMicBtn(){
  const btn=$('micBtn');
  if(!btn)return;
  _applyMicMode(false);

  function onDown(e){
    if(isRecording){return;}
    if(_vnRecording)return;
    if(processingFiles)return;
    e.preventDefault();
    _micLongFired=false;
    _micLongTimer=setTimeout(()=>{
      _micLongFired=true;
      if(_micMode==='video'){
        startVideoNote();
      }else{
        toggleRecording();
      }
    },300);
  }

  function onUp(e){
    if(_micLongTimer){clearTimeout(_micLongTimer);_micLongTimer=null;}
    if(_micLongFired)return;
    if(isRecording){stopRecordingForPreview();return;}
    if(_vnRecording){stopAndSendVideoNote();return;}
    _toggleMicMode();
  }

  btn.addEventListener('pointerdown',onDown,{passive:false});
  btn.addEventListener('pointerup',onUp);
  btn.addEventListener('pointercancel',()=>{if(_micLongTimer){clearTimeout(_micLongTimer);_micLongTimer=null;}});
  btn.addEventListener('contextmenu',e=>e.preventDefault());
}

// ── ЗАПИСЬ ВИДЕО-КРУЖКА ──
let _vnRecording=false;
let _vnStopping=false;
let _vnFlipping=false; // блокировка от закликивания (оставлено для совместимости)
let _vnPaused=false;   // запись на паузе
let _vnStream=null;
let _vnMediaRecorder=null;
let _vnChunks=[];
let _vnStartTime=0;
let _vnTimerIv=null;
let _vnFacingMode='user'; // только для первого старта и сброса
let _vnActivityHb=null;
// Список доступных видеокамер и текущий индекс
let _vnCameraDevices=[];
let _vnCameraIndex=0;
// Текущий mime-тип записи
let _vnMime='';

// Определяем mime один раз
function _vnPickMime(){
  // VP8 первым — на Android WebView он кодируется аппаратно почти всегда.
  // VP9 теоретически лучше по качеству, но программный кодировщик VP9
  // на слабом железе вызывает фризы. VP8 аппаратный = стабильный fps.
  const candidates=[
    'video/webm;codecs=vp8,opus',
    'video/webm;codecs=vp9,opus',
    'video/webm',
    'video/mp4',
    ''
  ];
  for(const m of candidates){if(!m||MediaRecorder.isTypeSupported(m))return m;}
  return '';
}

// Создаём MediaRecorder с оптимальным битрейтом
function _vnCreateRecorder(){
  // ── ПЛАВНОСТЬ TELEGRAM (Аппаратный VP8 + Высокий битрейт захвата) ──
  // Захватываем в 2.5 Mbps, чтобы на сервер пришло максимум деталей.
  // VP8 гарантирует аппаратное ускорение на большинстве Android/iOS WebView.
  const opts={};
  if(_vnMime)opts.mimeType=_vnMime;
  opts.videoBitsPerSecond=800_000; // 800 kbps — оптимальный битрейт для стабильного захвата на клиенте
  opts.audioBitsPerSecond=128_000;   // 128 kbps для звука
  opts.bitrateMode = 'constant'; // Попытка стабилизировать битрейт
  const rec=new MediaRecorder(_vnStream,opts);
  rec.ondataavailable=e=>{if(e.data&&e.data.size>0)_vnChunks.push(e.data);};
  rec.start(500); // Выдавать данные каждые 500 мс для лучшей синхронизации и плавности
  return rec;

}

// Является ли текущая камера фронтальной (для зеркала)
function _isFrontCamera(){
  if(_vnFacingMode==='environment')return false;
  if(_vnFacingMode==='user')return true;
  // По label если facingMode неизвестен
  const dev=_vnCameraDevices[_vnCameraIndex];
  if(!dev)return true;
  const lbl=(dev.label||'').toLowerCase();
  if(lbl.includes('back')||lbl.includes('rear')||lbl.includes('environment')||lbl.includes('задн'))return false;
  return true;
}

async function startVideoNote(){
  if(_vnRecording||isRecording||processingFiles)return;

  // Всегда начинаем с фронтальной
  _vnFacingMode='user';
  _vnCameraIndex=0;
  _vnCameraDevices=[];

  const ov=$('vnRecordOverlay');
  ov.classList.add('active');

  try{
    _vnStream=await navigator.mediaDevices.getUserMedia({
      video:{
        facingMode:'user',
        width: {exact:640},
        height:{exact:640},
        frameRate:{exact:30}, // Жестко задаем 30 FPS для максимальной плавности
        // resizeMode:'crop-and-scale', // Не нужен, если разрешение фиксировано
      },
      audio:{
        channelCount:1,
        sampleRate:48000,
        echoCancellation:false,
        noiseSuppression:false,
        autoGainControl:false,
        latency:0,
      }
    });
    // ── Разделяем аудио и видео, обрабатываем аудио и объединяем обратно ──
    const originalVideoTrack = _vnStream.getVideoTracks()[0];
    const originalAudioTrack = _vnStream.getAudioTracks()[0];
    const originalAudioStream = new MediaStream([originalAudioTrack]);
    const processedAudioStream = _applyAudioFiltersToStream(originalAudioStream);
    const processedAudioTrack = processedAudioStream.getAudioTracks()[0];
    
    const combinedStream = new MediaStream();
    combinedStream.addTrack(originalVideoTrack);
    combinedStream.addTrack(processedAudioTrack);
    _vnStream = combinedStream;

    const preview=$('vnPreview');
    preview.srcObject=_vnStream;
    preview.style.transform='scaleX(-1)'; // фронтальная — зеркало
    try{await preview.play();}catch(e){}

  }catch(e){
    // Fallback на 480p если устройство не тянет 720p
    try{
      _vnStream=await navigator.mediaDevices.getUserMedia({
        video:{
          facingMode:'user',
        width: {exact:480},
        height:{exact:480},
        frameRate:{exact:30},
        },
        audio:{
          channelCount:1,
          sampleRate:48000,
          echoCancellation:false,
          noiseSuppression:false,
          autoGainControl:false,
          latency:0,
        }
      });
      // ── Разделяем аудио и видео, обрабатываем аудио и объединяем обратно (fallback) ──
      const originalVideoTrack = _vnStream.getVideoTracks()[0];
      const originalAudioTrack = _vnStream.getAudioTracks()[0];
      const originalAudioStream = new MediaStream([originalAudioTrack]);
      const processedAudioStream = _applyAudioFiltersToStream(originalAudioStream);
      const processedAudioTrack = processedAudioStream.getAudioTracks()[0];
      
      const combinedStream = new MediaStream();
      combinedStream.addTrack(originalVideoTrack);
      combinedStream.addTrack(processedAudioTrack);
      _vnStream = combinedStream;

      const preview=$('vnPreview');
      preview.srcObject=_vnStream;
      preview.style.transform='scaleX(-1)'; // фронтальная — зеркало
      try{await preview.play();}catch(e){}

    }catch(e2){
      if(e2.name==='NotAllowedError'||e2.name==='PermissionDeniedError')toast('Нет доступа к камере/микрофону','err');
      else toast('Ошибка камеры','err');
      return;
    }
  }

  // После получения стрима — перечисляем устройства (теперь deviceId заполнен)
  try{
    const allDevices=await navigator.mediaDevices.enumerateDevices();
    _vnCameraDevices=allDevices.filter(d=>d.kind==='videoinput'&&d.deviceId&&d.deviceId!=='');
  }catch(e){_vnCameraDevices=[];}

  // Инициализируем кнопку паузы в исходное состояние
  const pauseBtnInit=$('vnPauseBtn');
  if(pauseBtnInit){pauseBtnInit.classList.remove('paused');pauseBtnInit.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <rect x=\"14\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> <rect x=\"5\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> </svg>';pauseBtnInit.title='Пауза';}

  // const preview=$('vnPreview'); // Перемещено выше
  // preview.srcObject=_vnStream; // Перемещено выше
  // preview.style.transform='scaleX(-1)'; // Перемещено выше
  // try{await preview.play();}catch(e){}

  _vnMime=_vnPickMime();
  _vnChunks=[];
  _vnMediaRecorder=_vnCreateRecorder();
  _vnRecording=true;
  _vnFlipping=false;
  _vnStartTime=Date.now();


  const ring=$('vnRingProg');
  ring.style.strokeDashoffset=VN_CIRCUMFERENCE;
  if(typeof _vib==='function')_vib('impactMedium');

  // requestAnimationFrame вместо setInterval — кольцо заполняется плавно 60fps
  // без рывков. Таймер (текст) обновляем только при смене секунды — не каждый кадр.
  let _lastSec=-1;
  function _vnRafTick(){
    if(!_vnRecording||_vnPaused)return; // пауза — не обновляем
    const elapsed=Date.now()-_vnStartTime;
    const sec=Math.floor(elapsed/1000);
    const timeEl=$('vnRecTime');
    if(sec!==_lastSec){
      _lastSec=sec;
      if(timeEl)timeEl.textContent=fmtDur(sec);
      const remainingSec=MAX_VN_SEC-sec;
      const isUrgent=remainingSec<=5&&remainingSec>=0;
      if(ring)ring.classList.toggle('urgent',isUrgent);
      if(timeEl)timeEl.classList.toggle('urgent',isUrgent);
      $('vnCircleWrap').classList.toggle('urgent',isUrgent);
    }
    const pct=Math.min(elapsed/(MAX_VN_SEC*1000),1);
    if(ring)ring.style.strokeDashoffset=(VN_CIRCUMFERENCE*(1-pct)).toFixed(3);
    if(elapsed>=MAX_VN_SEC*1000){stopAndSendVideoNote();return;}
    _vnTimerIv=requestAnimationFrame(_vnRafTick);
  }
  _vnTimerIv=requestAnimationFrame(_vnRafTick);

  if(activePid&&activePid!==MY_ID){
    _sendActivitySignal(activePid,'recording');
    _vnActivityHb=setInterval(()=>{if(activePid&&_vnRecording)_sendActivitySignal(activePid,'recording');},3000);
  }
}

function _stopVnInternal(){
  return new Promise(resolve=>{
    cancelAnimationFrame(_vnTimerIv);_vnTimerIv=null; // RAF handle
    clearInterval(_vnActivityHb);_vnActivityHb=null;
    _vnRecording=false;
    _vnFlipping=false;
    _vnPaused=false;
    // Сброс: следующая запись всегда начнётся с фронтальной камеры
    _vnFacingMode='user';
    _vnCameraIndex=0;
    _vnCameraDevices=[];
    _vnMime='';
    const ov=$('vnRecordOverlay');
    ov.classList.remove('active');
    $('vnRecTime').textContent='0:00';
    $('vnRecTime').classList.remove('urgent');
    $('vnRingProg').style.strokeDashoffset=VN_CIRCUMFERENCE;
    $('vnRingProg').classList.remove('urgent');
    $('vnCircleWrap').classList.remove('urgent','cancelling');
    const preview=$('vnPreview');
    preview.srcObject=null;
    // Сбрасываем состояние кнопки паузы
    const pauseBtn=$('vnPauseBtn');
    if(pauseBtn){pauseBtn.classList.remove('paused');pauseBtn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <rect x=\"14\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> <rect x=\"5\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> </svg>';pauseBtn.title='Пауза';}
    if(_vnStream){_vnStream.getTracks().forEach(t=>t.stop());_vnStream=null;}
    if(_vnMediaRecorder&&_vnMediaRecorder.state!=='inactive'){
      _vnMediaRecorder.addEventListener('stop',()=>resolve(_vnChunks),{once:true});
      _vnMediaRecorder.stop();
    }else{resolve(_vnChunks);}
  });
}

window.cancelVideoNote=async function(){
  if(!_vnRecording||_vnStopping)return;
  _vnStopping=true;
  // Если на паузе — возобновляем внутреннее состояние для корректной остановки
  if(_vnPaused){
    try{_vnMediaRecorder&&_vnMediaRecorder.state==='paused'&&_vnMediaRecorder.resume();}catch(e){}
    _vnPaused=false;
  }
  const circleWrap=$('vnCircleWrap');
  const cancelBtn=$('vnCancelBtn'),pauseBtn=$('vnPauseBtn');
  circleWrap.classList.remove('urgent');
  circleWrap.classList.add('cancelling');
  cancelBtn.classList.add('fading-out');
  if(pauseBtn)pauseBtn.classList.add('fading-out');
  if(typeof _vib==='function')_vib('notificationWarning');
  await new Promise(r=>setTimeout(r,220));
  await _stopVnInternal();
  circleWrap.classList.remove('cancelling');
  cancelBtn.classList.remove('fading-out');
  if(pauseBtn)pauseBtn.classList.remove('fading-out');
  _vnChunks=[];
  if(activePid)_sendActivityStop(activePid);
  toast('Запись отменена');
  _vnStopping=false;
};

window.stopAndSendVideoNote=async function(){
  if(!_vnRecording||_vnStopping)return;
  _vnStopping=true;
  const capturedStart=_vnStartTime;
  // Если на паузе — возобновляем перед финальной остановкой (чтобы собрать все чанки)
  if(_vnPaused){
    try{_vnMediaRecorder&&_vnMediaRecorder.state==='paused'&&_vnMediaRecorder.resume();}catch(e){}
    _vnPaused=false;
    await new Promise(r=>setTimeout(r,80));
  }
  const sendBtn=$('vnSendBtn'),cancelBtn=$('vnCancelBtn'),pauseBtn=$('vnPauseBtn');
  sendBtn.classList.add('launching');
  cancelBtn.classList.add('fading-out');
  if(pauseBtn)pauseBtn.classList.add('fading-out');
  if(typeof _vib==='function')_vib('notificationSuccess');
  await new Promise(r=>setTimeout(r,180));
  const chunks=await _stopVnInternal();
  sendBtn.classList.remove('launching');
  cancelBtn.classList.remove('fading-out');
  if(pauseBtn)pauseBtn.classList.remove('fading-out');
  _vnStopping=false;
  const durSec=Math.max(1,Math.round((Date.now()-capturedStart)/1000));
  if(!chunks||!chunks.length){toast('Пустая запись','warn');return;}
  const mimeType=chunks[0]?.type||'video/webm';
  const blob=new Blob(chunks,{type:mimeType});
  if(!blob.size){toast('Пустая запись','warn');return;}
  if(activePid)_sendActivityStop(activePid);
  try{
    const fileBuffer=await blob.arrayBuffer();
    await sendVideoNoteMessageBuffer(fileBuffer,durSec,mimeType,blob);
  }catch(e){
    toast('Ошибка подготовки видео-кружка','err');
    console.error('vn send error',e);
  }
};

// ── ПАУЗА / ПРОДОЛЖЕНИЕ ЗАПИСИ ВИДЕО-КРУЖКА ─────────────────────────────────
// Аналог resumeRecording() у голосовых — та же логика паузы MediaRecorder.
// ⏸ → ▶ (пауза) ; ▶ → ⏸ (продолжить запись)
window.togglePauseVideoNote=function(){
  if(!_vnRecording||_vnStopping)return;
  const pauseBtn=$('vnPauseBtn');
  const timeEl=$('vnRecTime');
  const circleWrap=$('vnCircleWrap');
  if(!_vnPaused){
    // ── Ставим на паузу ──
    try{
      if(_vnMediaRecorder&&_vnMediaRecorder.state==='recording'){
        _vnMediaRecorder.pause();
      }
    }catch(e){console.warn('vn pause err',e);}
    // Останавливаем RAF-таймер и activity heartbeat
    cancelAnimationFrame(_vnTimerIv);_vnTimerIv=null;
    // Замораживаем превью — ставим видеотрек на паузу
    const preview=$('vnPreview');
    if(preview&&!preview.paused)preview.pause();
    // Останавливаем activity heartbeat
    clearInterval(_vnActivityHb);_vnActivityHb=null;
    _vnPaused=true;
    // Визуальная обратная связь
    if(pauseBtn){pauseBtn.classList.add('paused');pauseBtn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>';pauseBtn.title='Продолжить запись';}
    if(timeEl)timeEl.classList.add('paused');
    if(circleWrap)circleWrap.classList.add('paused');
    if(typeof _vib==='function')_vib('impactLight');
    if(activePid)_sendActivityStop(activePid);
  }else{
    // ── Продолжаем запись ──
    try{
      if(_vnMediaRecorder&&_vnMediaRecorder.state==='paused'){
        _vnMediaRecorder.resume();
      }
    }catch(e){console.warn('vn resume err',e);}
    // Возобновляем превью
    const preview=$('vnPreview');
    if(preview&&preview.paused)preview.play().catch(()=>{});
    _vnPaused=false;
    // Перезапускаем RAF-таймер от текущей позиции
    // (_vnStartTime остаётся неизменным — отсчёт от начала записи)
    const ring=$('vnRingProg');
    let _resumeLastSec=-1;
    function _vnRafResume(){
      if(!_vnRecording||_vnPaused)return;
      const elapsed=Date.now()-_vnStartTime;
      const sec=Math.floor(elapsed/1000);
      if(sec!==_resumeLastSec){
        _resumeLastSec=sec;
        if(timeEl)timeEl.textContent=fmtDur(sec);
        const remainingSec=MAX_VN_SEC-sec;
        const isUrgent=remainingSec<=5&&remainingSec>=0;
        if(ring)ring.classList.toggle('urgent',isUrgent);
        if(timeEl)timeEl.classList.toggle('urgent',isUrgent);
        if(circleWrap)circleWrap.classList.toggle('urgent',isUrgent);
      }
      const pct=Math.min(elapsed/(MAX_VN_SEC*1000),1);
      if(ring)ring.style.strokeDashoffset=(VN_CIRCUMFERENCE*(1-pct)).toFixed(3);
      if(elapsed>=MAX_VN_SEC*1000){stopAndSendVideoNote();return;}
      _vnTimerIv=requestAnimationFrame(_vnRafResume);
    }
    _vnTimerIv=requestAnimationFrame(_vnRafResume);
    // Возобновляем activity heartbeat
    if(activePid&&activePid!==MY_ID){
      _sendActivitySignal(activePid,'recording');
      _vnActivityHb=setInterval(()=>{if(activePid&&_vnRecording&&!_vnPaused)_sendActivitySignal(activePid,'recording');},3000);
    }
    // Визуальная обратная связь
    if(pauseBtn){pauseBtn.classList.remove('paused');pauseBtn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <rect x=\"14\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> <rect x=\"5\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> </svg>';pauseBtn.title='Пауза';}
    if(timeEl)timeEl.classList.remove('paused');
    if(circleWrap)circleWrap.classList.remove('paused');
    if(typeof _vib==='function')_vib('impactLight');
  }
};

// ── ОТПРАВКА ВИДЕО-КРУЖКА ЧЕРЕЗ ЧАНКОВЫЙ ПРОТОКОЛ ──────────────────────────
// Проблема: base64 видео в одном зашифрованном WS-сообщении → слишком большой
// payload, WebView/сервер молча дропает или получатель видит чёрный кружок.
// Решение: используем тот же store-file-header / store-chunks / file-available
// протокол что и для обычных файлов. Имя файла — «__vnote__<msgId>»,
// по которому получатель распознаёт видео-кружок и рендерит его правильно.
const VN_FILE_PREFIX = '__vnote__';

async function sendVideoNoteMessageBuffer(fileBuffer, durSec, mimeType, blob) {
  const msgId = uid();
  const ts = Date.now();

  // Сохраняем blob локально
  try { await saveBlob(msgId + '_vnote', fileBuffer); } catch(e) {}

  // ObjectURL для немедленного рендера у отправителя
  let previewURL = null;
  try { previewURL = URL.createObjectURL(blob || new Blob([fileBuffer],{type:mimeType})); } catch(e) {}

  // Режим "Избранное"
  if (activePid === MY_ID) {
    const m = {
      id: msgId, text: '', type: 'sent',
      time: new Date(ts).toISOString(),
      reactions: {}, edited: false, replyTo: replyTo || null,
      delivered: true, forwarded: false,
      videoNote: true, voiceDuration: durSec, voiceListened: true,
      videoBlobId: msgId + '_vnote', videoMime: mimeType
    };
    await upsertMsg(MY_ID, m);
    await updateChat(MY_ID, { lastMsg: 'Видео-кружок', lastMsgTime: ts });
    appendOrReloadMsg(MY_ID, { ...m, videoData: previewURL, _previewURL: previewURL });
    cancelReply();
    return;
  }

  if (!wsUp) { toast('Нет связи с сервером', 'err'); return; }

  const mSender = {
    id: msgId, text: '', type: 'sent',
    time: new Date(ts).toISOString(),
    reactions: {}, edited: false, replyTo: replyTo || null,
    delivered: false, forwarded: false,
    videoNote: true, voiceDuration: durSec, voiceListened: false,
    videoBlobId: msgId + '_vnote', videoMime: mimeType
  };
  await upsertMsg(activePid, mSender);
  await updateChat(activePid, { lastMsg: 'Видео-кружок', lastMsgTime: ts });
  appendOrReloadMsg(activePid, { ...mSender, videoData: previewURL, _previewURL: previewURL });
  cancelReply();
  _sendActivityStop(activePid);

  const vnMeta = JSON.stringify({ durSec, videoMime: mimeType, replyTo: replyTo || null, ts });
  const totalChunks = Math.ceil(fileBuffer.byteLength / CHUNK_SIZE);
  const fileId = msgId;

  // ОПТИМИЗАЦИЯ 1: генерируем thumb для видео-кружка
  let vnThumb = '';
  try {
    if (blob || fileBuffer) {
      const vnBlob = blob || new Blob([fileBuffer], {type: mimeType});
      vnThumb = await generateThumb({type: mimeType, blob: vnBlob}).catch(()=>'');
    }
  } catch(e) {}

  wsSend({type:'get-file-status',fileId});
  const status = await new Promise(resolve=>{
    const timer=setTimeout(()=>resolve({exists:false}),5000);
    storeAckWaiters.set('status_'+fileId,{resolve:(res)=>{clearTimeout(timer);resolve(res);}});
  });
  storeAckWaiters.delete('status_'+fileId);

  let receivedChunks = new Set();
  if(status.exists){
    receivedChunks = new Set(status.receivedChunks);
  } else {
    wsSend({
      type: 'store-file-header',
      fileId,
      recipientId: activePid,
      name: VN_FILE_PREFIX + msgId,
      size: fileBuffer.byteLength,
      mimeType: mimeType || 'video/webm',
      totalChunks,
      caption: vnMeta,
      thumb: vnThumb,
      ts
    });
    await new Promise(resolve => {
      const timer = setTimeout(resolve, 4000);
      storeAckWaiters.set(fileId, { resolve: () => { clearTimeout(timer); resolve(); }, timer });
    });
    storeAckWaiters.delete(fileId);
  }

  let vnKey = null;
  try { vnKey = await getKey(activePid); } catch(e) {}

  for (let i = 0; i < totalChunks; i++) {
    if(receivedChunks.has(i)) continue;
    const start = i * CHUNK_SIZE;
    const slice = fileBuffer.slice(start, Math.min(start + CHUNK_SIZE, fileBuffer.byteLength));
    let chunkData;
    if (vnKey) {
      const encBuf = await encData(slice, vnKey);
      chunkData = arrayBufferToBase64(encBuf);
    } else {
      chunkData = arrayBufferToBase64(slice);
    }
    wsSend({ type: 'store-chunks', fileId, chunks: [{ index: i, data: chunkData }] });
    
    await new Promise(resolve=>{
      const timer=setTimeout(resolve,30000);
      storeAckWaiters.set('chunk_'+fileId+'_'+i,{resolve:()=>{clearTimeout(timer);resolve();}});
    });
    storeAckWaiters.delete('chunk_'+fileId+'_'+i);
  }
}

// Старая функция оставлена как fallback (dataURL → buffer)
async function sendVideoNoteMessage(dataURL, durSec, mimeType) {
  let fileBuffer;
  try {
    const b64 = dataURL.replace(/^data:[^,]+,/, '');
    fileBuffer = base64ToArrayBuffer(b64);
  } catch(e) {
    toast('Ошибка подготовки видео-кружка', 'err');
    return;
  }
  const blob = new Blob([fileBuffer], { type: mimeType });
  await sendVideoNoteMessageBuffer(fileBuffer, durSec, mimeType, blob);
}

// ── ОТОБРАЖЕНИЕ ВИДЕО-КРУЖКА В ЧАТЕ ──
let _currentVnVideo=null;
let _currentVnReset=null;
const VN_SMALL=152;
const VN_BIG=268;

function buildVideoNoteBubble(m,isSent){
  const totalSec=m.voiceDuration||0;
  const dotHidden=!!m.voiceListened;
  const wrap=document.createElement('div');
  wrap.className='vn-bubble-wrap '+(isSent?'sent':'recv');
  const vid=document.createElement('video');
  vid.className='vn-thumb-video';
  vid.playsInline=true;
  vid.preload='metadata';
  vid.loop=false;
  vid.muted=false;
  vid.style.cssText=`position:absolute;top:0;left:0;width:100%;height:100%;object-fit:cover;display:block;transform:scale(1.06);transform-origin:center center;`;

  // ── FIX: загрузка видео с правильным управлением ObjectURL ──
  let _activeObjectURL=null;
  function _revokeObjectURL(){if(_activeObjectURL){URL.revokeObjectURL(_activeObjectURL);_activeObjectURL=null;}}

  // ── FIX чёрного кружка ──────────────────────────────────────────────────
  // На многих Android WebView (включая Median.co) <video> с preload="metadata"
  // не рисует первый кадр пока не начнётся воспроизведение — элемент остаётся
  // полностью чёрным. Решение: после loadeddata принудительно "перематываем"
  // на крошечный таймкод (0.001с) — это форсирует декодирование и отрисовку
  // кадра без видимого воспроизведения. Применяется один раз при первой загрузке.
  let _posterExtracted=false;
  function _extractPosterFrame(){
    if(_posterExtracted)return;
    _posterExtracted=true;
    try{
      if(vid.currentTime===0){
        // Сикаем на крошечный сдвиг — браузер декодирует и отрисует кадр
        vid.currentTime=0.001;
      }
    }catch(e){}
  }
  vid.addEventListener('loadeddata',_extractPosterFrame);
  vid.addEventListener('loadedmetadata',_extractPosterFrame);
  // На случай если события не сработали (некоторые WebView) — фоллбек по таймауту
  vid.addEventListener('canplay',_extractPosterFrame);

  function _setVideoSrc(src){
    _revokeObjectURL();
    _posterExtracted=false;
    vid.src=src;
    vid.load();
  }

  // ── PENDING: видео-кружок ещё не скачан — показываем кнопку скачивания ──
  // (как у файловых миниатюр), вместо автоматической загрузки в фоне.
  const isPending=!!m._vnPending;
  let dlOverlay=null;

  function _buildDownloadOverlay(){
    const ov=document.createElement('div');
    ov.className='vn-dl-wrap';
    const btn=document.createElement('div');
    btn.className='vn-dl-btn';
    btn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M12 15V3\" /> <path d=\"M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4\" /> <path d=\"m7 10 5 5 5-5\" /> </svg>';
    btn.title='Скачать видео-кружок';
    btn.onclick=e=>{
      e.stopPropagation();
      _startVnoteDownload();
    };
    ov.appendChild(btn);
    return ov;
  }

  function _buildSpinnerOverlay(){
    const ov=document.createElement('div');
    ov.className='vn-dl-wrap';
    const spinner=document.createElement('div');
    spinner.className='vn-dl-spinner';
    spinner.dataset.fileid=m.id;
    spinner.innerHTML=`<svg viewBox="0 0 42 42"><circle class="vn-dl-spinner-track" cx="21" cy="21" r="19"/><circle class="vn-dl-spinner-fill" cx="21" cy="21" r="19" stroke-dasharray="122" stroke-dashoffset="122"/></svg><div class="vn-dl-spinner-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M12 15V3" /> <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /> <path d="m7 10 5 5 5-5" /> </svg></div>`;
    ov.appendChild(spinner);
    return ov;
  }

  function _startVnoteDownload(){
    if(!dlOverlay)return;
    const spinnerOv=_buildSpinnerOverlay();
    dlOverlay.replaceWith(spinnerOv);
    dlOverlay=spinnerOv;
    const videoMime=m.videoMime||'video/webm';
    _downloadVideoNote(m.id,m._vnSenderId||(isSent?activePid:m._vnSenderId)||activePid,videoMime).catch(()=>{
      toast('Ошибка загрузки видео-кружка','err');
      const dlOv=_buildDownloadOverlay();
      dlOverlay.replaceWith(dlOv);
      dlOverlay=dlOv;
    });
  }

  if(isPending){
    dlOverlay=_buildDownloadOverlay();
  } else if(m._videoDataForRender && (m._videoDataForRender.startsWith('data:')||m._videoDataForRender.startsWith('blob:'))){
    // Временное поле для рендера — blob уже сохранён, используем dataURL/objectURL для немедленного показа
    _setVideoSrc(m._videoDataForRender);
  } else if(m.videoData && (m.videoData.startsWith('data:')||m.videoData.startsWith('blob:')||m.videoData.startsWith('http'))){
    // dataURL или ObjectURL доступен напрямую (свежее сообщение отправителя)
    _setVideoSrc(m.videoData);
  } else if(m.videoBlobId){
    // Загружаем из IndexedDB — создаём ObjectURL и держим его живым
    (async()=>{
      try{
        const buf=await loadBlob(m.videoBlobId);
        if(!buf){console.warn('vn blob not found:',m.videoBlobId);return;}
        const mime=m.videoMime||'video/webm';
        const blob=new Blob([buf],{type:mime});
        _activeObjectURL=URL.createObjectURL(blob);
        _setVideoSrc(_activeObjectURL);
      }catch(e){console.error('vn blob load error',e);}
    })();
  }

  // Освобождаем ObjectURL когда элемент убирается из DOM
  const observer=new MutationObserver(()=>{
    if(!document.contains(wrap)){_revokeObjectURL();observer.disconnect();}
  });
  observer.observe(document.body,{childList:true,subtree:true});

  function makeSvgRing(sz){
    // sz = размер внутреннего кружка (circleWrap).
    // SVG живёт в outer (sz+8) → viewBox и пиксели тоже sz+8.
    // Кольцо: cx=cy=(sz+8)/2, r=(sz+8)/2 - strokeW/2 → чётко по краю outer.
    const outerSz = sz + 8;
    const strokeW = 5;
    const cx = outerSz / 2;
    const cy = outerSz / 2;
    const r  = cx - strokeW / 2;
    const circ = 2 * Math.PI * r;
    const svg = document.createElementNS('http://www.w3.org/2000/svg','svg');
    svg.setAttribute('viewBox', `0 0 ${outerSz} ${outerSz}`);
    svg.style.cssText = `position:absolute;top:0;left:0;width:${outerSz}px;height:${outerSz}px;pointer-events:none;overflow:visible;`;
    svg.classList.add('vn-ring-svg-play');
    svg.style.opacity = '0';
    const track = document.createElementNS('http://www.w3.org/2000/svg','circle');
    track.setAttribute('cx', cx+''); track.setAttribute('cy', cy+''); track.setAttribute('r', r+'');
    track.setAttribute('fill','none');
    track.setAttribute('stroke','rgba(255,255,255,.25)');
    track.setAttribute('stroke-width', strokeW+'');
    const prog = document.createElementNS('http://www.w3.org/2000/svg','circle');
    prog.setAttribute('cx', cx+''); prog.setAttribute('cy', cy+''); prog.setAttribute('r', r+'');
    prog.setAttribute('fill','none');
    prog.setAttribute('stroke','var(--g)');
    prog.setAttribute('stroke-width', strokeW+'');
    prog.setAttribute('stroke-linecap','round');
    prog.style.cssText = `transform:rotate(-90deg);transform-origin:${cx}px ${cy}px;`;
    prog.setAttribute('stroke-dasharray', circ.toFixed(2));
    prog.setAttribute('stroke-dashoffset', circ.toFixed(2));
    svg.appendChild(track); svg.appendChild(prog);
    return {svg, prog, circ, r, cx, cy, sz, outerSz};
  }

  let curSz=VN_SMALL;
  let svgObj=makeSvgRing(curSz);
  // pauseDot как SVG-circle внутри кольцевого SVG — не обрезается
  let pauseDotEl=document.createElementNS('http://www.w3.org/2000/svg','circle');
  pauseDotEl.setAttribute('r','6');
  pauseDotEl.setAttribute('fill','#fff');
  pauseDotEl.setAttribute('stroke','rgba(0,0,0,.25)');
  pauseDotEl.setAttribute('stroke-width','1');
  pauseDotEl.style.cssText='opacity:0;transition:opacity .18s;';
  svgObj.svg.appendChild(pauseDotEl);

  // ── Новая структура DOM: outer → (inner круг с overflow:hidden + SVG кольцо поверх) ──
  // outer — без overflow:hidden, размер задаётся здесь
  const outer=document.createElement('div');
  outer.className='vn-thumb-outer';
  outer.style.cssText=`width:${VN_SMALL+8}px;height:${VN_SMALL+8}px;`;

  // inner — overflow:hidden, вписан внутрь outer с отступом 4px под кольцо
  const circleWrap=document.createElement('div');
  circleWrap.className='vn-thumb-circle';
  // top/left/right/bottom через CSS — абсолютный внутри outer

  circleWrap.appendChild(vid);

  outer.appendChild(circleWrap);
  // SVG кольцо поверх — position:absolute внутри outer, не обрезается
  outer.appendChild(svgObj.svg);

  // Кнопка/спиннер скачивания (если кружок ещё не загружен)
  if(dlOverlay)outer.appendChild(dlOverlay);

  // ── Overlay-плашки на кружке (Telegram-стиль, как у обычного видео) ──
  // 1. Длительность + dot непросмотренного — левый нижний угол
  const durPill=document.createElement('div');
  durPill.className='vn-dur-pill';
  const dot=document.createElement('span');
  dot.className='vn-unread-dot'+(dotHidden?' hidden':'');
  const timerEl=document.createElement('span');
  timerEl.textContent=fmtDur(totalSec);
  durPill.appendChild(dot);
  durPill.appendChild(timerEl);

  // 2. Время + галочки — правый нижний угол
  const metaPill=document.createElement('div');
  metaPill.className='vn-meta-pill';
  const timeEl=document.createElement('span');
  timeEl.className='msg-time-inline';
  timeEl.textContent=new Date(m.time).toLocaleTimeString('ru',{hour:'2-digit',minute:'2-digit'});
  metaPill.appendChild(timeEl);
  if(isSent){const tk=document.createElement('span');tk.className='msg-ticks'+(m.delivered?' double':' single');tk.innerHTML = m.delivered?'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M18 6 7 17l-5-5" /> <path d="m22 10-7.5 7.5L13 16" /> </svg>':'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M20 6 9 17l-5-5" /> </svg>';metaPill.appendChild(tk);}

  outer.appendChild(durPill);
  outer.appendChild(metaPill);

  wrap.appendChild(outer);

  let expanded=false,playing=false,rafId=null;
  let _ringReady=false; // true когда кольцо разрешено показывать (после завершения разворота)
  function _setPlaying(v){playing=v;wrap.classList.toggle('vn-playing',v);}
  // Обработчик клика вне кружка для сворачивания
  let _outsideClickHandler=null;

  function _removeOutsideClick(){
    if(_outsideClickHandler){
      document.removeEventListener('click',_outsideClickHandler,true);
      _outsideClickHandler=null;
    }
  }

  function _attachOutsideClick(){
    _removeOutsideClick();
    _outsideClickHandler=function(e){
      // Если клик не попал на circleWrap и его потомков — сворачиваем
      if(!outer.contains(e.target)){
        e.stopPropagation();
        _doReset();
        _removeOutsideClick();
      }
    };
    // capture:true чтобы поймать клик раньше других обработчиков
    setTimeout(()=>document.addEventListener('click',_outsideClickHandler,true),50);
  }

  function _getEffDur(){if(totalSec>0)return totalSec;const d=vid.duration;return(d&&isFinite(d)&&d>0)?d:0;}

  function _updateRing(){
    const dur=_getEffDur();
    const cur=vid.currentTime||0;
    const pct=dur>0?Math.min(cur/dur,1):0;
    svgObj.prog.setAttribute('stroke-dashoffset',(svgObj.circ*(1-pct)).toFixed(2));
    timerEl.textContent=dur>0?fmtDur(Math.floor(cur))+' / '+fmtDur(Math.round(dur)):fmtDur(Math.floor(cur));
    // pauseDot: SVG-circle на конце дуги
    if(!playing&&expanded&&cur>0){
      const angle=pct*2*Math.PI-Math.PI/2;
      const px=svgObj.cx+svgObj.r*Math.cos(angle);
      const py=svgObj.cy+svgObj.r*Math.sin(angle);
      pauseDotEl.setAttribute('cx',px.toFixed(2));
      pauseDotEl.setAttribute('cy',py.toFixed(2));
      pauseDotEl.style.opacity='1';
    }else{
      pauseDotEl.style.opacity='0';
    }
  }

  function rafTick(){if(!playing){rafId=null;return;}_updateRing();rafId=requestAnimationFrame(rafTick);}

  function _rebuildRing(sz,keepOpacity){
    const prevOpacity = svgObj.svg.style.opacity || '0';
    if(svgObj.svg.parentNode) svgObj.svg.parentNode.removeChild(svgObj.svg);
    curSz = sz; svgObj = makeSvgRing(sz);
    const dur = _getEffDur(), cur = vid.currentTime||0, pct = dur>0 ? Math.min(cur/dur,1) : 0;
    svgObj.prog.setAttribute('stroke-dashoffset', (svgObj.circ*(1-pct)).toFixed(2));
    svgObj.svg.style.opacity = keepOpacity ? prevOpacity : '0';
    pauseDotEl = document.createElementNS('http://www.w3.org/2000/svg','circle');
    pauseDotEl.setAttribute('r','6');
    pauseDotEl.setAttribute('fill','#fff');
    pauseDotEl.setAttribute('stroke','rgba(0,0,0,.25)');
    pauseDotEl.setAttribute('stroke-width','1');
    pauseDotEl.style.cssText = 'opacity:0;transition:opacity .18s;';
    svgObj.svg.appendChild(pauseDotEl);
    outer.insertBefore(svgObj.svg, dlOverlay||null);
  }

  function _doReset(){
    if(playing)vid.pause();
    _setPlaying(false);
    if(rafId){cancelAnimationFrame(rafId);rafId=null;}
    pauseDotEl.style.opacity='0';
    svgObj.svg.style.opacity='0';
    // Сворачивание по тапу СНАРУЖИ кружка — это не "пауза", а явный выход
    // из просмотра. При следующем открытии видео должно начаться сначала
    // (в отличие от паузы тапом по самому кружку, где позиция сохраняется).
    try{vid.currentTime=0;}catch(e){}
    _updateRing();
    setTimeout(()=>{if(expanded)_collapse();},300);
  }

  function _expand(){
    expanded=true;curSz=VN_BIG;_ringReady=false;
    wrap.classList.add('expanded');
    // Меняем размер outer — inner (circleWrap) тянется через CSS inset
    outer.style.width=(VN_BIG+8)+'px';outer.style.height=(VN_BIG+8)+'px';
    _rebuildRing(VN_BIG,false);
    // Кольцо появляется ТОЛЬКО когда кружок уже полностью развернулся —
    // иначе оно мгновенно отрисовывается в финальном (большом) размере
    // и "плывёт" отдельно от ещё увеличивающегося бабла.
    let _ringRevealed=false;
    const onResized=(e)=>{
      if(e&&e.propertyName!=='width')return;
      if(_ringRevealed)return;
      _ringRevealed=true;
      outer.removeEventListener('transitionend',onResized);
      _ringReady=true;
      svgObj.svg.style.opacity='1';
    };
    outer.addEventListener('transitionend',onResized);
    // Фоллбэк на случай если transitionend не сработает (например, если
    // изменение размера было отменено очень коротким тапом)
    setTimeout(onResized,420);
    _attachOutsideClick();
  }

  function _collapse(){
    expanded=false;curSz=VN_SMALL;_ringReady=false;
    wrap.classList.remove('expanded');
    outer.style.width=(VN_SMALL+8)+'px';outer.style.height=(VN_SMALL+8)+'px';
    _rebuildRing(VN_SMALL,false);
    svgObj.prog.setAttribute('stroke-dashoffset',svgObj.circ+'');
    pauseDotEl.style.opacity='0';
    timerEl.textContent=fmtDur(totalSec);
    _removeOutsideClick();
  }

  function _startPlay(){
    if(_currentVnReset&&_currentVnReset!==_doReset)_currentVnReset();
    _currentVnReset=_doReset;
    if(_currentVnVideo&&_currentVnVideo!==vid&&!_currentVnVideo.paused)_currentVnVideo.pause();
    if(typeof currentAudio!=='undefined'&&currentAudio&&!currentAudio.paused)currentAudio.pause();
    _currentVnVideo=vid;

    // Ждём пока видео готово перед play()
    const doPlay=()=>{
      // Сбрасываем currentTime если он был смещён только для извлечения poster-кадра
      if(vid.currentTime>0&&vid.currentTime<0.05&&!playing){
        try{vid.currentTime=0;}catch(e){}
      }
      vid.play().then(()=>{
        _setPlaying(true);pauseDotEl.style.opacity='0';
        // Кольцо появляется при начале воспроизведения — но только если
        // разворот уже завершён (иначе кольцо покажет onResized из _expand,
        // чтобы не "мигало" в финальном размере посреди анимации разворота)
        if(_ringReady)svgObj.svg.style.opacity='1';
        if(rafId)cancelAnimationFrame(rafId);
        rafTick();
        // ── Помечаем просмотренным (только для получателя) ──
        // Отправитель просматривает своё видео — это НЕ влияет на индикатор
        // "просмотрено получателем", точно как у голосовых сообщений.
        if(!isSent&&!m.voiceListened){
          m.voiceListened=true;
          dot.classList.add('hidden');       // dot в meta-row
          // Получатель: сохраняем в IDB + шлём сигнал отправителю
          const _pid=activePid;
          withChatLock(_pid,async()=>{const msgs=await loadMsgs(_pid);const mm=msgs.find(x=>x.id===m.id);if(mm){mm.voiceListened=true;await saveMsgs(_pid,msgs);}}).catch(()=>{});
          // Сигнал отправителю что видео просмотрено
          if(activePid&&activePid!==MY_ID)sendVnWatched(activePid,m.id);
        }
      }).catch(e=>{
        console.error('vn play error',e);
        // Если src не загружен — пробуем перезагрузить
        if(!vid.src&&m.videoBlobId){
          (async()=>{
            const buf=await loadBlob(m.videoBlobId);
            if(!buf)return;
            _revokeObjectURL();
            const blob=new Blob([buf],{type:m.videoMime||'video/webm'});
            _activeObjectURL=URL.createObjectURL(blob);
            vid.src=_activeObjectURL;
            vid.load();
            vid.addEventListener('canplay',()=>vid.play().catch(()=>{}),{once:true});
          })();
        }
      });
    };

    if(vid.readyState>=2){doPlay();}
    else{vid.addEventListener('canplay',doPlay,{once:true});}
  }

  vid.addEventListener('ended',()=>{
    _setPlaying(false);if(rafId){cancelAnimationFrame(rafId);rafId=null;}
    pauseDotEl.style.opacity='0';
    vid.currentTime=0;
    // Кольцо заполнено до конца — плавно скрываем, потом сворачиваем
    svgObj.prog.setAttribute('stroke-dashoffset','0');
    timerEl.textContent=fmtDur(totalSec);
    _removeOutsideClick();
    setTimeout(()=>{
      svgObj.svg.style.opacity='0';
      setTimeout(()=>{if(!playing)_collapse();},300);
    },600);
    if(_currentVnReset===_doReset)_currentVnReset=null;
    if(_currentVnVideo===vid)_currentVnVideo=null;
  });

  vid.addEventListener('error',e=>{
    // При ошибке загрузки — пробуем перечитать blob
    if(m.videoBlobId&&!_activeObjectURL){
      (async()=>{
        try{
          const buf=await loadBlob(m.videoBlobId);
          if(!buf)return;
          const blob=new Blob([buf],{type:m.videoMime||'video/webm'});
          _activeObjectURL=URL.createObjectURL(blob);
          vid.src=_activeObjectURL;
          vid.load();
        }catch(err){}
      })();
    }
  });

  outer.addEventListener('click',e=>{
    e.stopPropagation();
    if(isPending)return; // ждём скачивания — клик обрабатывает кнопка ⬇ внутри overlay
    if(!expanded){_expand();_startPlay();}
    else if(playing){vid.pause();_setPlaying(false);if(rafId){cancelAnimationFrame(rafId);rafId=null;}_updateRing();}
    else{pauseDotEl.style.opacity='0';if(vid.ended||vid.currentTime===0)vid.currentTime=0;_startPlay();}
  });

  return wrap;
}

// ── ИНИЦИАЛИЗАЦИЯ МИК-КНОПКИ ──
document.addEventListener('DOMContentLoaded',()=>setTimeout(_initMicBtn,0));

// ── VOICE PLAYBACK ──
let currentAudio=null;
let currentVoiceReset=null; // функция сброса волны текущего голосового плеера

// ══════════════════════════════════════════════════════
// ══════════════════════════════════════════════════════
// ── КАСТОМНЫЙ АУДИО-ПЛЕЕР (Telegram-стиль) ──
// Встроен прямо в bubble без отдельной file-card сверху.
// Структура: [Play] [название / полоса / таймер+мета]
// ══════════════════════════════════════════════════════
function buildAudioPlayer(file, isSent, metaEl){
  const wrap = document.createElement('div');
  wrap.className = 'audio-bubble';

  // ── Кнопка Play/Pause (46×46, круглая — как в Telegram) ──
  const btn = document.createElement('button');
  btn.className = 'audio-play-btn';
  btn.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>';
  btn.setAttribute('aria-label','play');

  // ── Правая колонка ──
  const right = document.createElement('div');
  right.className = 'audio-right';

  // Название файла (без расширения, с эллипсисом)
  const title = document.createElement('div');
  title.className = 'audio-title';
  title.textContent = file.name
    ? file.name.replace(/\.[^/.]+$/, '')
    : 'Аудио';

  // Полоса перемотки
  const seekWrap = document.createElement('div');
  seekWrap.className = 'audio-seek-wrap';
  const track = document.createElement('div');
  track.className = 'audio-seek-track';
  const fill = document.createElement('div');
  fill.className = 'audio-seek-fill';
  const thumb = document.createElement('div');
  thumb.className = 'audio-seek-thumb';
  seekWrap.appendChild(track);
  seekWrap.appendChild(fill);
  seekWrap.appendChild(thumb);

  // Нижняя строка: таймер + мета (время сообщения + галочки)
  const bottomRow = document.createElement('div');
  bottomRow.className = 'audio-bottom';
  const timer = document.createElement('span');
  timer.className = 'audio-timer';
  timer.textContent = '0:00 / 0:00';
  const metaWrap = document.createElement('span');
  metaWrap.className = 'audio-msg-meta';
  if(metaEl) metaWrap.appendChild(metaEl);
  bottomRow.appendChild(timer);
  bottomRow.appendChild(metaWrap);

  right.appendChild(title);
  right.appendChild(seekWrap);
  right.appendChild(bottomRow);
  wrap.appendChild(btn);
  wrap.appendChild(right);

  // ── Аудио-логика ──
  let audio = null;
  let isPlaying = false;
  let rafId = null;
  let duration = 0;

  function fmtT(s){
    s = Math.max(0, Math.floor(s));
    return `${Math.floor(s/60)}:${String(s%60).padStart(2,'0')}`;
  }
  function getEffDur(){
    if(duration > 0) return duration;
    const d = audio?.duration;
    return (d && isFinite(d) && d > 0) ? d : 0;
  }
  function updateUI(){
    if(!audio) return;
    const dur = getEffDur();
    const cur = audio.currentTime || 0;
    const pct = dur > 0 ? Math.min(cur / dur, 1) : 0;
    const W   = seekWrap.offsetWidth || 0;
    const R   = 6.5; // радиус шарика (13px / 2)
    const thumbLeft = pct * (W - R * 2); // от 0 до (W - 13)
    fill.style.width = (thumbLeft + R) + 'px'; // до центра шарика
    thumb.style.left = thumbLeft + 'px';
    timer.textContent = `${fmtT(cur)} / ${fmtT(dur)}`;
    if(isPlaying) rafId = requestAnimationFrame(updateUI);
  }
  function _initAudioWithSrc(src){
    if(audio) return;
    audio = new Audio(src);
    audio.addEventListener('loadedmetadata', () => {
      duration = (audio.duration && isFinite(audio.duration)) ? audio.duration : 0;
      timer.textContent = `0:00 / ${fmtT(duration)}`;
    });
    audio.onended = () => {
      isPlaying = false;
      btn.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>';
      btn.classList.remove('playing');
      cancelAnimationFrame(rafId);
      fill.style.width = '0px';
      thumb.style.left = '0px';
      timer.textContent = `0:00 / ${fmtT(getEffDur())}`;
    };
    audio.onpause = () => {
      isPlaying = false;
      btn.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>';
      btn.classList.remove('playing');
      cancelAnimationFrame(rafId);
    };
  }
  async function initAudio(){
    if(audio) return;
    // Обратная совместимость: data URL (старый формат) или blobId
    if(file.data){
      _initAudioWithSrc(file.data);
    } else if(file.blobId){
      const url = await _blobToObjectURL(file.blobId, file.type||'audio/mpeg');
      if(url) _initAudioWithSrc(url);
    }
  }

  btn.addEventListener('click', e => {
    e.stopPropagation();
    if(currentAudio && currentAudio !== audio && !currentAudio.paused)
      currentAudio.pause();
    initAudio();
    if(isPlaying){
      audio.pause();
    } else {
      audio.play().then(() => {
        isPlaying = true;
        btn.innerHTML = '<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <rect x=\"14\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> <rect x=\"5\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> </svg>';
        btn.classList.add('playing');
        currentAudio = audio;
        cancelAnimationFrame(rafId);
        updateUI();
      }).catch(console.error);
    }
  });

  // Seek: клик, drag, touch
  function applySeek(clientX){
    initAudio();
    const r   = seekWrap.getBoundingClientRect();
    const R   = 6.5; // радиус шарика
    const W   = r.width;
    // Кликаем по треку — pct считаем относительно зоны движения шарика
    const pct = Math.max(0, Math.min((clientX - r.left - R) / (W - R * 2), 1));
    const dur = getEffDur();
    if(dur > 0){
      audio.currentTime  = pct * dur;
      const thumbLeft    = pct * (W - R * 2);
      fill.style.width   = (thumbLeft + R) + 'px';
      thumb.style.left   = thumbLeft + 'px';
      timer.textContent  = `${fmtT(audio.currentTime)} / ${fmtT(dur)}`;
    }
  }
  let seekDrag = false;
  seekWrap.addEventListener('click',      e => { e.stopPropagation(); applySeek(e.clientX); });
  seekWrap.addEventListener('touchstart', e => { seekDrag = true; applySeek(e.touches[0].clientX); }, {passive:true});
  seekWrap.addEventListener('touchmove',  e => { if(seekDrag) applySeek(e.touches[0].clientX); }, {passive:true});
  seekWrap.addEventListener('touchend',   ()=> { seekDrag = false; });

  // НЕ инициализируем аудио сразу — только при нажатии Play или попадании во viewport
  // Это критично для производительности при большом количестве аудио-сообщений
  if('IntersectionObserver' in window){
    const _audioIo=new IntersectionObserver(entries=>{
      if(entries[0].isIntersecting){_audioIo.disconnect();initAudio().catch(()=>{});}
    },{threshold:0.1,rootMargin:'200px'});
    _audioIo.observe(wrap);
  }
  // else: initAudio вызовется при первом клике на Play
  return wrap;
}


// ══════════════════════════════════════════════════════
// КАСТОМНЫЙ ВИДЕОПЛЕЕР (Telegram-стиль)
// Превью в чате — динамический размер как у фото.
// Тап → открывается fullscreen lightbox с прогресс-баром.
// ══════════════════════════════════════════════════════
(function initVideoLightbox(){
  const lb      = $('vidLightbox');
  const video   = $('vidLbVideo');
  const fill    = $('vidLbFill');
  const thumb   = $('vidLbThumb');
  const timer   = $('vidLbTimer');
  const seekWrap= $('vidLbSeekWrap');
  const closeBtn= $('vidLbClose');
  const tapIcon = $('vidLbTapIcon');
  if(!lb||!video) return;

  let rafId = null;
  let seekDrag = false;

  function fmtT(s){
    s = Math.max(0, Math.floor(s||0));
    return `${Math.floor(s/60)}:${String(s%60).padStart(2,'0')}`;
  }
  function getEffDur(){
    const d = video.duration;
    return (d && isFinite(d) && d > 0) ? d : 0;
  }
  function updateSeek(){
    const dur = getEffDur();
    const cur = video.currentTime || 0;
    const pct = dur > 0 ? Math.min(cur/dur,1)*100 : 0;
    fill.style.width  = pct + '%';
    thumb.style.left  = pct + '%';
    timer.textContent = `${fmtT(cur)} / ${fmtT(dur)}`;
    if(!video.paused && !video.ended) rafId = requestAnimationFrame(updateSeek);
  }
  function startRaf(){ cancelAnimationFrame(rafId); rafId = requestAnimationFrame(updateSeek); }
  function stopRaf() { cancelAnimationFrame(rafId); }

  function applySeek(clientX){
    const r = seekWrap.getBoundingClientRect();
    const pct = Math.max(0, Math.min((clientX - r.left) / r.width, 1));
    const dur = getEffDur();
    if(dur > 0){
      video.currentTime = pct * dur;
      fill.style.width  = (pct*100)+'%';
      thumb.style.left  = (pct*100)+'%';
      timer.textContent = `${fmtT(video.currentTime)} / ${fmtT(dur)}`;
    }
  }

  // Seek events
  seekWrap.addEventListener('click', e=>{e.stopPropagation(); applySeek(e.clientX);});
  seekWrap.addEventListener('touchstart', e=>{seekDrag=true; applySeek(e.touches[0].clientX);},{passive:true});
  seekWrap.addEventListener('touchmove',  e=>{if(seekDrag)applySeek(e.touches[0].clientX);},{passive:true});
  seekWrap.addEventListener('touchend',   ()=>{seekDrag=false;});

  // Тап по видео — пауза/воспроизведение
  let tapIconTimer = null;
  video.addEventListener('click', e=>{
    e.stopPropagation();
    if(video.paused || video.ended){ video.play(); }
    else { video.pause(); }
  });
  video.addEventListener('play',  ()=>{ tapIcon.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>'; showTapIcon(); startRaf(); });
  video.addEventListener('pause', ()=>{ tapIcon.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <rect x=\"14\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> <rect x=\"5\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> </svg>'; showTapIcon(); stopRaf(); });
  video.addEventListener('ended', ()=>{
    stopRaf();
    fill.style.width='0%'; thumb.style.left='0%';
    timer.textContent=`0:00 / ${fmtT(getEffDur())}`;
    tapIcon.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>'; showTapIcon();
  });
  video.addEventListener('loadedmetadata',()=>{
    timer.textContent=`0:00 / ${fmtT(getEffDur())}`;
  });
  video.addEventListener('timeupdate',()=>{
    if(!rafId) updateSeek();
  });

  function showTapIcon(){
    clearTimeout(tapIconTimer);
    tapIcon.classList.add('show');
    tapIconTimer = setTimeout(()=>tapIcon.classList.remove('show'), 700);
  }

  // Закрытие
  closeBtn.addEventListener('click', e=>{e.stopPropagation(); window.closeVideoLightbox();});
  lb.addEventListener('click', e=>{if(e.target===lb) window.closeVideoLightbox();});
  document.addEventListener('keydown', e=>{ if(e.key==='Escape' && lb.classList.contains('open')) window.closeVideoLightbox(); });

  window.openVideoLightbox = (src) => {
    video.src = src;
    video.currentTime = 0;
    fill.style.width='0%'; thumb.style.left='0%';
    timer.textContent='0:00 / 0:00';
    lb.classList.add('open');
    // Автовоспроизведение с небольшой задержкой после открытия
    setTimeout(()=>{ video.play().catch(()=>{}); }, 120);
  };

  window.closeVideoLightbox = () => {
    video.pause();
    stopRaf();
    lb.classList.remove('open');
    setTimeout(()=>{ video.src=''; fill.style.width='0%'; thumb.style.left='0%'; }, 300);
  };
})();


function buildVideoPlayer(file, isSent, m){
  // ── Внешняя обёртка (как media-bubble у фото) ──
  const outerWrap = document.createElement('div');
  outerWrap.className = 'vid-bubble';

  const MAX_W = Math.min(260, window.innerWidth * 0.72);
  const MAX_H = 320;
  const MIN_W = 120;

  // Функция которая устанавливает размер обёртки
  function applySizeFromVideo(vw, vh){
    if(!vw||!vh) return;
    const ratio = vw/vh;
    let w, h;
    if(ratio >= 1){
      w = Math.min(vw, MAX_W);
      h = w / ratio;
      if(h > MAX_H){ h = MAX_H; w = h * ratio; }
    } else {
      h = Math.min(vh, MAX_H);
      w = h * ratio;
      if(w > MAX_W){ w = MAX_W; h = w / ratio; }
      if(w < MIN_W){ w = MIN_W; h = w / ratio; }
    }
    w = Math.round(w); h = Math.round(h);
    outerWrap.style.width  = w + 'px';
    outerWrap.style.height = h + 'px';
    thumb.style.width  = w + 'px';
    thumb.style.height = h + 'px';
  }

  // Скрытый video-тег только для получения размеров и постера
  const hiddenVid = document.createElement('video');
  hiddenVid.preload = 'metadata';
  hiddenVid.style.cssText = 'position:absolute;width:0;height:0;opacity:0;pointer-events:none';
  hiddenVid.muted = true;

  // Превью-изображение (canvas-постер из первого кадра)
  const thumb = document.createElement('canvas');
  thumb.className = 'vid-bubble-thumb';
  thumb.style.cssText = `width:${MIN_W}px;height:${Math.round(MIN_W*9/16)}px;display:block;background:#111;border-radius:0`;

  // Кнопка Play поверх превью
  const playOverlay = document.createElement('div');
  playOverlay.className = 'vid-play-overlay';
  const playBtn = document.createElement('div');
  playBtn.className = 'vid-play-btn';
  playBtn.innerHTML = `<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z" /> </svg>`;
  playOverlay.appendChild(playBtn);

  // Значок длительности (левый верхний угол)
  const durBadge = document.createElement('div');
  durBadge.className = 'vid-dur-badge';
  durBadge.textContent = '0:00';

  // Мета-overlay (время+галочки, правый нижний угол)
  const metaOverlay = document.createElement('div');
  metaOverlay.className = 'vid-meta-overlay';
  const tEl = document.createElement('span');
  tEl.className = 'msg-time-inline';
  tEl.textContent = new Date(m.time).toLocaleTimeString('ru',{hour:'2-digit',minute:'2-digit'});
  metaOverlay.appendChild(tEl);
  if(isSent){
    const tk = document.createElement('span');
    tk.className = 'msg-ticks' + (m.delivered?' double':' single');
    tk.innerHTML = m.delivered ? '<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M18 6 7 17l-5-5" /> <path d="m22 10-7.5 7.5L13 16" /> </svg>' : '<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M20 6 9 17l-5-5" /> </svg>';
    metaOverlay.appendChild(tk);
  }

  outerWrap.appendChild(hiddenVid);
  outerWrap.appendChild(thumb);
  outerWrap.appendChild(playOverlay);
  outerWrap.appendChild(durBadge);
  outerWrap.appendChild(metaOverlay);

  // Начальные размеры до загрузки
  outerWrap.style.width  = MIN_W + 'px';
  outerWrap.style.height = Math.round(MIN_W*9/16) + 'px';

  // Функция получения постера из первого кадра
  function captureFrame(videoEl){
    try{
      const vw = videoEl.videoWidth, vh = videoEl.videoHeight;
      if(!vw||!vh) return;
      applySizeFromVideo(vw, vh);
      thumb.width  = parseInt(outerWrap.style.width);
      thumb.height = parseInt(outerWrap.style.height);
      const ctx = thumb.getContext('2d');
      ctx.drawImage(videoEl, 0, 0, thumb.width, thumb.height);
    }catch(e){}
  }

  // Src загрузка
  let videoSrc = null;
  const _initWithSrc = (src) => {
    videoSrc = src;
    hiddenVid.src = src;
    hiddenVid.currentTime = 0.001; // тригерит первый кадр
    hiddenVid.addEventListener('seeked', function onSeeked(){
      hiddenVid.removeEventListener('seeked', onSeeked);
      captureFrame(hiddenVid);
      const dur = hiddenVid.duration;
      if(dur && isFinite(dur)){
        const s = Math.floor(dur);
        durBadge.textContent = `${Math.floor(s/60)}:${String(s%60).padStart(2,'0')}`;
      }
    },{once:true});
    hiddenVid.addEventListener('loadedmetadata', function onMeta(){
      hiddenVid.removeEventListener('loadedmetadata', onMeta);
      const vw=hiddenVid.videoWidth, vh=hiddenVid.videoHeight;
      if(vw&&vh) applySizeFromVideo(vw, vh);
      const dur = hiddenVid.duration;
      if(dur && isFinite(dur)){
        const s = Math.floor(dur);
        durBadge.textContent = `${Math.floor(s/60)}:${String(s%60).padStart(2,'0')}`;
      }
    });
  };

  if(file.data){
    _initWithSrc(file.data);
  } else if(file._tempUrl){
    _initWithSrc(file._tempUrl);
  } else if(file.blobId){
    const load = async () => {
      if(file._tempUrl){ _initWithSrc(file._tempUrl); return; }
      const url = await _blobToObjectURL(file.blobId, file.type||'video/mp4');
      if(url) _initWithSrc(url);
    };
    if('IntersectionObserver' in window){
      const io = new IntersectionObserver(en=>{if(en[0].isIntersecting){io.disconnect();load();}},{threshold:0.1});
      io.observe(outerWrap);
    } else { load(); }
  }

  // Тап по превью — открываем fullscreen плеер
  outerWrap.addEventListener('click', e=>{
    e.stopPropagation();
    if(!videoSrc) return;
    window.openVideoLightbox(videoSrc);
  });

  return outerWrap;
}


function buildVoiceBubble(m,isSent){
  // totalSec — сохранённая длительность из метаданных записи.
  // Используем её как основу для прогресс-бара, т.к. audio.duration
  // для data: URL может быть Infinity/NaN пока браузер не декодирует весь файл.
  const totalSec=m.voiceDuration||0;const dotHidden=!!m.voiceListened;
  const wrap=document.createElement('div');wrap.className='voice-bubble';
  const btn=document.createElement('button');btn.className='voice-play-btn';btn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>';
  const right=document.createElement('div');right.className='voice-right';
  const wf=document.createElement('div');wf.className='voice-waveform';
  const waveform=m.voice?.waveform||Array.from({length:WF_BARS},()=>Math.random());
  const barEls=[];
  // Всегда рисуем полоску-волну из баров.
  // В режиме энергосбережения CSS-правила body.power-save отключают анимации
  // (wfPulse, transitions) — бары остаются статичными без доп. нагрузки.
  const DISP=Math.min(waveform.length,WF_BARS);
  for(let i=0;i<DISP;i++){
    const b=document.createElement('div');
    b.className='wf-bar';
    b.style.height=Math.max(3,Math.round((waveform[i]||0.2)*22))+'px';
    wf.appendChild(b);
    barEls.push(b);
  }
  const bot=document.createElement('div');bot.className='voice-bottom';
  const timerEl=document.createElement('span');timerEl.className='voice-timer';timerEl.textContent=fmtDur(totalSec);
  const dot=document.createElement('span');dot.className='voice-unread-dot'+(dotHidden?' hidden':'');
  const metaWrap=document.createElement('span');metaWrap.className='voice-meta-inline';
  const timeEl=document.createElement('span');timeEl.className='msg-time-inline';timeEl.textContent=new Date(m.time).toLocaleTimeString('ru',{hour:'2-digit',minute:'2-digit'});
  metaWrap.appendChild(timeEl);
  if(isSent){const tk=document.createElement('span');tk.className='msg-ticks'+(m.delivered?' double':' single');tk.innerHTML = m.delivered?'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M18 6 7 17l-5-5" /> <path d="m22 10-7.5 7.5L13 16" /> </svg>':'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M20 6 9 17l-5-5" /> </svg>';metaWrap.appendChild(tk);}
  bot.appendChild(timerEl);bot.appendChild(dot);bot.appendChild(metaWrap);
  right.appendChild(wf);right.appendChild(bot);wrap.appendChild(btn);wrap.appendChild(right);
  let audio=null,isPlaying=false,rafId=null;

  function updateProgress(pct){
    const count=Math.round(pct*barEls.length);
    barEls.forEach((b,i)=>{
      b.classList.toggle('played',i<count);
      b.classList.toggle('active-bar',i===count-1&&count>0);
    });
  }

  // Полный сброс волны + таймера — используется при переключении на другой плеер
  function resetWave(){
    if(rafId){cancelAnimationFrame(rafId);rafId=null;}
    isPlaying=false;
    btn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>';btn.classList.remove('playing');
    timerEl.textContent=fmtDur(totalSec);
    updateProgress(0);
  }

  function getEffectiveDuration(){
    // Приоритет: сохранённая длительность записи (всегда точная),
    // затем audio.duration если он уже известен и конечен
    if(totalSec>0) return totalSec;
    const d=audio?.duration;
    return(d&&isFinite(d)&&d>0)?d:0;
  }

  function rafTick(){
    if(!audio||audio.paused){rafId=null;return;}
    const dur=getEffectiveDuration();
    const cur=audio.currentTime||0;
    const pct=dur>0?Math.min(cur/dur,1):0;
    timerEl.textContent=`${fmtDur(Math.floor(cur))} / ${fmtDur(Math.round(dur))}`;
    updateProgress(pct);
    rafId=requestAnimationFrame(rafTick);
  }

  function doPlay(){
    // Сбрасываем волну предыдущего голосового плеера (если он отличается от нас)
    if(currentVoiceReset && currentVoiceReset !== resetWave){
      currentVoiceReset();
    }
    currentVoiceReset=resetWave;
    if(currentAudio&&currentAudio!==audio&&!currentAudio.paused)currentAudio.pause();
    if(!audio){
      // Обратная совместимость: data URL или blobId
      const _initWithSrc=src=>{
        if(!src){toast('Голосовое недоступно','err');return;}
        audio=new Audio(src);
        currentAudio=audio;
        audio.onended=()=>{
          isPlaying=false;
          if(rafId){cancelAnimationFrame(rafId);rafId=null;}
          btn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>';btn.classList.remove('playing');
          timerEl.textContent=fmtDur(totalSec);
          // Сбрасываем волну — сообщение уже прослушано, заливка исчезает
          updateProgress(0);
          // По окончании — следующее нажатие Play должно начинать СНАЧАЛА,
          // а не с конца дорожки. Пауза (см. onpause) позицию не трогает.
          try{audio.currentTime=0;}catch(e){}
          if(currentVoiceReset===resetWave) currentVoiceReset=null;
        };
        audio.onpause=()=>{
          isPlaying=false;
          if(rafId){cancelAnimationFrame(rafId);rafId=null;}
          btn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <path d=\"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z\" /> </svg>';btn.classList.remove('playing');
        };
        audio.addEventListener('loadedmetadata',()=>{if(isPlaying)rafTick();});
        _startPlay();
      };
      if(m.voice.data){_initWithSrc(m.voice.data);}
      else if(m.voice.blobId){_blobToObjectURL(m.voice.blobId,'audio/webm').then(_initWithSrc);}
      else{toast('Голосовое недоступно','err');}
      return; // _startPlay вызовется изнутри _initWithSrc
    }
    _startPlay();
  }
  function _startPlay(){
    if(!audio)return;
    // Запускаем rafTick сразу при нажатии Play — до загрузки метаданных
    audio.play().then(async()=>{
      isPlaying=true;
      btn.innerHTML='<svg class=\"lucide-icon lucide\" xmlns=\"http://www.w3.org/2000/svg\" width=\"1em\" height=\"1em\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" > <rect x=\"14\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> <rect x=\"5\" y=\"3\" width=\"5\" height=\"18\" rx=\"1\" /> </svg>';btn.classList.add('playing');
      currentAudio=audio;
      if(rafId) cancelAnimationFrame(rafId);
      rafTick();
      if(!isSent&&!m.voiceListened){
        m.voiceListened=true;
        const pidForLock=activePid;
        await withChatLock(pidForLock,async()=>{
          const msgs=await loadMsgs(pidForLock);
          const mm=msgs.find(x=>x.id===m.id);
          if(mm){mm.voiceListened=true;await saveMsgs(pidForLock,msgs);}
        });
        dot.classList.add('hidden');
        if(activePid!==MY_ID)sendVoiceListened(activePid,m.id);
      }
    }).catch(e=>console.error(e));
  }

  btn.addEventListener('click',e=>{
    e.stopPropagation();
    if(isPlaying)audio.pause();else doPlay();
  });

  wf.addEventListener('click',e=>{
    if(!audio){doPlay();return;}
    const dur=getEffectiveDuration();
    if(dur>0){
      const r=wf.getBoundingClientRect();
      const pct=Math.max(0,Math.min((e.clientX-r.left)/r.width,1));
      audio.currentTime=pct*dur;
      if(!isPlaying){
        // Обновляем визуально без воспроизведения
        timerEl.textContent=`${fmtDur(Math.floor(audio.currentTime))} / ${fmtDur(Math.round(dur))}`;
        updateProgress(pct);
      }
    }
  });

  return wrap;
}

// ══════════════════════════════════════════════════════
// ── СИСТЕМА ЗАКРЕПЛЁННЫХ СООБЩЕНИЙ (Telegram-стиль) ──
// ══════════════════════════════════════════════════════
// Хранилище: localStorage bc_pinned_<peerId> → массив {id, text, ts, pinnedAt}
// Последнее закреплённое отображается вверху; тап на баре циклически листает
// по всем закреплённым (от последнего к первому) и подсвечивает сообщение.

function _pinnedKey(pid){return`bc_pinned_${pid}`;}

function getPinnedMsgs(pid){
  return lsGet(_pinnedKey(pid),[]);
}

function _savePinnedMsgs(pid,arr){
  lsSet(_pinnedKey(pid),arr);
}

// Индекс текущего «показываемого» закреплённого (для цикличного листания)
// -1 = показываем последнее (по умолчанию)
let _pinnedViewIdx = -1;

// Текущий peerId для которого показана панель (чтобы сбрасывать при смене чата)
let _pinnedBarPid = '';

// Закрепить сообщение и оповестить собеседника
async function pinMessage(m){
  if(!activePid)return;
  const pins=getPinnedMsgs(activePid);
  if(pins.find(p=>p.id===m.id)){toast('Уже закреплено');return;}
  // Краткий превью текста
  const preview=_pinPreview(m);
  pins.push({id:m.id,text:preview,ts:m.time,pinnedAt:Date.now()});
  _savePinnedMsgs(activePid,pins);
  _pinnedViewIdx=-1; // сбрасываем на последнее
  renderPinnedBar(activePid);
  _markRowPinned(m.id,true);
  toast('Сообщение закреплено');
  // Синхронизируем с собеседником
  await _sendPinSignal(activePid,'pin',m.id,preview,m.time);
}

// Открепить сообщение (по ID) и оповестить собеседника
async function unpinMessage(msgId){
  if(!activePid)return;
  let pins=getPinnedMsgs(activePid);
  pins=pins.filter(p=>p.id!==msgId);
  _savePinnedMsgs(activePid,pins);
  _pinnedViewIdx=-1;
  renderPinnedBar(activePid);
  _markRowPinned(msgId,false);
  toast('Сообщение откреплено');
  await _sendPinSignal(activePid,'unpin',msgId,null,null);
}

// Краткий превью для закреплённой панели
function _pinPreview(m){
  if(m.voice)return'Голосовое сообщение';
  if(m.media)return'Фотография';
  if(m.file)return`${m.file.name||'Файл'}`;
  if(m.fileAvailable)return`${m.fileAvailable.name||'Файл'}`;
  return(m.text||'').replace(/\n/g,' ').slice(0,80)||'Сообщение';
}

// Помечаем/снимаем метку закреплённого у DOM-строки
function _markRowPinned(msgId,isPinned){
  const row=document.querySelector(`.msg-row[data-msgid="${msgId}"]`);
  if(row){row.classList.toggle('pinned-msg',isPinned);
    // Inject/remove pin badge SVG
    const bubble = row.querySelector('.bubble');
    if(bubble){
      let badge = bubble.querySelector('.pinned-pin-badge');
      if(isPinned){
        if(!badge){
          badge = document.createElement('span');
          badge.className = 'pinned-pin-badge';
          badge.innerHTML = `<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M12 17v5" /> <path d="M9 10.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H8a2 2 0 0 0 0 4 1 1 0 0 1 1 1z" /> </svg>`;
          bubble.insertBefore(badge, bubble.firstChild);
        }
      } else {
        if(badge) badge.remove();
      }
    }}
}

// Перерисовать панель закреплённого вверху чата
function renderPinnedBar(pid){
  const bar=$('pinnedBar');
  const previewEl=$('pinnedPreview');
  const labelEl=$('pinnedLabel');
  if(!bar||!previewEl||!labelEl)return;
  const pins=getPinnedMsgs(pid);
  if(!pins.length){
    bar.classList.remove('visible');
    _pinnedBarPid='';
    return;
  }
  _pinnedBarPid=pid;
  // Показываем последнее закреплённое (или то, на котором остановились)
  const idx=_getDisplayPinIdx(pins);
  const pin=pins[idx];
  // Счётчик если закреплено больше одного
  if(pins.length>1){
    labelEl.textContent=`Закреплено · ${idx+1} / ${pins.length}`;
  }else{
    labelEl.textContent='Закреплено';
  }
  previewEl.textContent=pin.text;
  bar.classList.add('visible');
}

// Получить индекс для отображения
// _pinnedViewIdx указывает на то сообщение, к которому ПЕРЕЙДЁМ при следующем тапе
// (оно же сейчас показано на панели)
function _getDisplayPinIdx(pins){
  if(_pinnedViewIdx<0||_pinnedViewIdx>=pins.length)return pins.length-1;
  return _pinnedViewIdx;
}

// Клик по закреплённой панели (Telegram-логика):
//   1. Прокручиваем к тому, что СЕЙЧАС показано на панели
//   2. После этого переключаем панель на следующее сообщение в очереди
window.onPinnedBarClick=async function(){
  if(!activePid)return;
  const pins=getPinnedMsgs(activePid);
  if(!pins.length)return;

  // Шаг 1: какое сообщение сейчас показано → к нему и прокручиваем
  const curIdx=_getDisplayPinIdx(pins);
  const pin=pins[curIdx];

  // Шаг 2: вычисляем СЛЕДУЮЩИЙ индекс (от последнего к первому, по кругу)
  const nextIdx=(curIdx-1+pins.length)%pins.length;
  _pinnedViewIdx=nextIdx;

  // Шаг 3: обновляем панель (теперь показывает следующее)
  renderPinnedBar(activePid);

  // Шаг 4: прокручиваем к тому сообщению на которое нажали
  await _scrollAndHighlightMsg(pin.id);
};

// Закрытие / открепление по кнопке × на панели
window.onPinnedCloseClick=async function(e){
  e.stopPropagation();
  if(!activePid)return;
  const pins=getPinnedMsgs(activePid);
  if(!pins.length)return;
  const idx=_getDisplayPinIdx(pins);
  const pin=pins[idx];
  showConfirm({
    icon:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M12 17v5" /> <path d="M15 9.34V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H7.89" /> <path d="m2 2 20 20" /> <path d="M9 9v1.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h11" /> </svg>',
    title:'Открепить сообщение?',
    text:'Сообщение будет откреплено у всех участников чата.',
    yesLabel:'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M12 17v5" /> <path d="M15 9.34V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H7.89" /> <path d="m2 2 20 20" /> <path d="M9 9v1.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h11" /> </svg> Открепить',
    noLabel:'Отмена',
    onYes:()=>unpinMessage(pin.id)
  });
};

// ── Подсветка всей строки на полную ширину чата ──────────────────────────
// Создаём абсолютный div-оверлей в .messages-area, выровненный по позиции
// целевой строки. Работает для любого контента: текст, стикер, голосовое.
function _doMsgRowHighlight(targetRow){
  if(!targetRow)return;
  // Убираем старую подсветку если есть (повторный тап)
  document.querySelectorAll('.msg-row.row-highlighted').forEach(r=>{
    r.style.removeProperty('--hl-top');
    r.style.removeProperty('--hl-height');
    r.classList.remove('row-highlighted');
  });
  // Убираем старый overlay-div если остался от предыдущей версии
  const old=document.querySelector('.msg-row-hl');
  if(old)old.remove();

  // Вычисляем реальные визуальные границы строки включая overflow-контент.
  // Стикеры рендерятся с font-size:72px + overflow:visible — они выходят за
  // пределы .msg-row, поэтому нужно найти самый большой дочерний элемент.
  const rowRect=targetRow.getBoundingClientRect();
  let topMost=rowRect.top;
  let bottomMost=rowRect.bottom;
  targetRow.querySelectorAll('*').forEach(child=>{
    const r=child.getBoundingClientRect();
    if(r.width===0&&r.height===0)return;
    if(r.top<topMost)topMost=r.top;
    if(r.bottom>bottomMost)bottomMost=r.bottom;
  });

  // Смещение top и высота relative к .msg-row (может быть отрицательным сверху)
  const hlTop=topMost-rowRect.top;
  const hlHeight=bottomMost-topMost;

  // Передаём в ::before через CSS-переменные
  targetRow.style.setProperty('--hl-top',`${hlTop}px`);
  targetRow.style.setProperty('--hl-height',`${hlHeight}px`);
  targetRow.classList.add('row-highlighted');

  const cleanup=()=>{
    targetRow.style.removeProperty('--hl-top');
    targetRow.style.removeProperty('--hl-height');
    targetRow.classList.remove('row-highlighted');
  };
  targetRow.addEventListener('animationend',cleanup,{once:true});
  // Страховка: принудительно убираем через 2.7s если animationend не сработал
  setTimeout(cleanup,2700);
}

// Прокрутка + подсветка (используется при тапе на закреплённое и ответ)
async function _scrollAndHighlightMsg(msgId){
  const sp=$('stickerPanel');
  const panelWasOpen=sp&&sp.classList.contains('open');

  if(panelWasOpen){
    // Блокируем открытие панели пока идёт навигация
    window._stickerNavLock=true;
    // Мгновенно замораживаем и скрываем панель без ожидания анимации
    sp.classList.add('frozen');
    sp.classList.remove('open');
    document.removeEventListener('click',_stickerOutsideClick);
    const se=$('stickerSearch');if(se)se.value='';
    stickerSearchQuery='';
    const inputBar=$('chatInputBar');
    if(inputBar)inputBar.classList.remove('sticker-mode');
    // Даём браузеру один frame применить стили до scrollIntoView
    await new Promise(res=>requestAnimationFrame(()=>requestAnimationFrame(res)));
  }

  let targetRow=document.querySelector(`.msg-row[data-msgid="${msgId}"]`);
  // Если строки нет в DOM (старое сообщение) — пробуем загрузить историю
  if(!targetRow){
    await loadChatHistory(activePid,false);
    targetRow=document.querySelector(`.msg-row[data-msgid="${msgId}"]`);
  }
  if(!targetRow){
    window._stickerNavLock=false;
    if(sp)sp.classList.remove('frozen');
    return;
  }
  targetRow.scrollIntoView({behavior:'smooth',block:'center'});
  // Подсветка всей строки — с задержкой чтобы smooth scroll успел отработать
  setTimeout(()=>_doMsgRowHighlight(targetRow), 300);
  // Снимаем блокировку через 600ms
  setTimeout(()=>{
    window._stickerNavLock=false;
    if(sp)sp.classList.remove('frozen');
  },600);
}

// Синхронизация закрепления через зашифрованный envelope
async function _sendPinSignal(pid,action,msgId,preview,msgTs){
  if(!wsUp||pid===MY_ID)return;
  const key=keyCache[pid]||await loadPersistedKey(pid);
  if(!key)return;
  try{
    const payload={type:'pin-msg',action,msgId,preview:preview||'',msgTs:msgTs||null,ts:Date.now()};
    const enc=await encData(ENC.encode(JSON.stringify(payload)),key);
    wsSend({type:'send-msg',target:pid,msgId:uid(),payload:payloadToB64(enc)});
  }catch(e){}
}

// Обработка входящего pin-сигнала (в handleEnvelope)
async function handleInPinMsg(from,env){
  if(env.action==='pin'){
    const pins=getPinnedMsgs(from);
    if(!pins.find(p=>p.id===env.msgId)){
      pins.push({id:env.msgId,text:env.preview||'Сообщение',ts:env.msgTs,pinnedAt:env.ts});
      _savePinnedMsgs(from,pins);
    }
  }else if(env.action==='unpin'){
    const pins=getPinnedMsgs(from).filter(p=>p.id!==env.msgId);
    _savePinnedMsgs(from,pins);
  }
  if(currentScreen==='scr-chat'&&activePid===from){
    _pinnedViewIdx=-1;
    renderPinnedBar(from);
    // Обновляем иконки закреплённых в DOM
    if(env.action==='pin')_markRowPinned(env.msgId,true);
    else _markRowPinned(env.msgId,false);
  }
}

// Инициализация панели при открытии чата
function initPinnedBar(pid){
  _pinnedViewIdx=-1;
  _pinnedBarPid='';
  renderPinnedBar(pid);
  // Помечаем все закреплённые сообщения в текущем DOM
  const pins=getPinnedMsgs(pid);
  document.querySelectorAll('.msg-row[data-msgid]').forEach(row=>{
    const mid=row.dataset.msgid;
    row.classList.toggle('pinned-msg',pins.some(p=>p.id===mid));
  });
}

// ══════════════════════════════════════════════════════

// ── INIT ──
// Восстанавливаем pending fileId→peerId после перезагрузки
try{const _sftp=lsGet('_fileSentToPeer',{});Object.entries(_sftp).forEach(([fid,pid])=>_fileSentToPeer.set(fid,pid));}catch(e){}
loadSavedTheme();
initFavoritesChat().then(async()=>{
  await migrateLocalStorageMessages();
  await warmAllChatKeys();
  initAvt();goScreen('scr-home');renderChatList();renderContacts();ensureWS();
});

// ── МИГРАЦИЯ: переносим данные из localStorage в IndexedDB v3 ──
// 1. Сообщения из bc_msgs_* → IDB 'messages' (если IDB пуст)
// 2. Чаты из bc_chats → IDB 'meta' (если IDB 'meta' пуст)
async function migrateLocalStorageMessages(){
  try{
    const d=await openMessagesDB();

    // ── Миграция чатов ──
    try{
      const existingChats=await _metaGet(CHATS_META_KEY);
      if(!existingChats||!existingChats.length){
        const lsChats=lsGet('bc_chats',null);
        if(lsChats&&lsChats.length){
          await _metaPut(CHATS_META_KEY,lsChats);
          console.log(`[migrate] Chats (${lsChats.length}) → IDB meta`);
        }
      }
    }catch(e){ console.warn('[migrate] chats error',e); }

    // ── Миграция сообщений ──
    const keys=Object.keys(localStorage).filter(k=>k.startsWith('bc_msgs_'));
    for(const k of keys){
      const pid=k.replace('bc_msgs_','');
      try{
        const existing=await new Promise((res,rej)=>{
          const req=d.transaction(STORE_NAME,'readonly').objectStore(STORE_NAME).get(pid);
          req.onsuccess=()=>res(req.result);req.onerror=()=>rej(req.error);
        });
        if(!existing||!existing.messages||!existing.messages.length){
          const msgs=JSON.parse(localStorage.getItem(k)||'[]');
          if(msgs.length){
            await saveMsgs(pid,msgs);
            console.log(`[migrate] ${msgs.length} msgs for ${pid} → IDB`);
          }
        }
      }catch(e){ console.warn('[migrate] msgs error for',pid,e); }
    }
  }catch(e){ console.warn('[migrate] failed',e); }
}

// ══════════════════════════════════════════════════════════════════
// ── СТИКЕРЫ — данные и логика вынесены в stickers-data.js ──
// (STICKER_NAMES, STICKER_PACKS, getRecentStickers, addRecentSticker,
//  ПЕРЕД этим скриптом)
// ══════════════════════════════════════════════════════════════════

let stickerActivePack=1; // 0 = recent, 1 = smileys
let stickerSearchQuery='';

function openStickerPanel(){
  if(!activePid||isContactBlocked(activePid))return;
  // Блокируем открытие если сейчас идёт навигация к сообщению
  if(window._stickerNavLock)return;
  // Закрываем панель вложений если открыта
  const asheet=$('attachSheet');
  if(asheet&&asheet.classList.contains('open'))closeAttachSheet();
  const panel=$('stickerPanel');
  if(!panel)return;
  // Если есть недавние — показываем их, иначе смайлики
  const rec=getRecentStickers();
  if(rec.length>0){
    STICKER_PACKS[0].stickers=rec;
    stickerActivePack=0;
  }else{
    stickerActivePack=1;
  }
  renderStickerTabs();
  renderStickerGrid(stickerActivePack,'');
  panel.classList.remove('frozen');
  panel.classList.add('open');
  $('messageInput').blur();
  if(!panel._listenersAttached){
    panel._listenersAttached=true;
    const closeBtn=$('stickerPanelCloseBtn');
    if(closeBtn){
      closeBtn.addEventListener('click',(e)=>{e.stopPropagation();closeStickerPanel();});
    }
    const searchInput=$('stickerSearch');
    if(searchInput){
      searchInput.addEventListener('input',()=>{
        const q=(searchInput.value||'').trim();
        stickerSearchQuery=q;
        renderStickerGrid(stickerActivePack,q);
      });
      searchInput.addEventListener('click',(e)=>e.stopPropagation());
      searchInput.addEventListener('focus',(e)=>e.stopPropagation());
    }

    // ── Drag-to-close (свайп вниз за ручку / шапку панели) ──────────────────
    // Аналог attach sheet: тянем вниз → панель следует за пальцем → закрывается
    const dragHandle=panel.querySelector('.sticker-drag-handle');
    const dragHeader=panel.querySelector('.sticker-panel-header');
    // Зона перетаскивания — и ручка и вся шапка с табами
    const dragZones=[dragHandle,dragHeader].filter(Boolean);

    let _spDragStartY=0,_spDragCurrentY=0,_spDragging=false;

    function _spDragStart(clientY){
      _spDragStartY=clientY;
      _spDragCurrentY=clientY;
      _spDragging=true;
      panel.style.transition='none'; // отключаем CSS-анимацию на время drag
    }
    function _spDragMove(clientY){
      if(!_spDragging)return;
      _spDragCurrentY=clientY;
      const dy=Math.max(0,clientY-_spDragStartY); // только вниз
      panel.style.transform=`translateY(${dy}px)`;
    }
    function _spDragEnd(){
      if(!_spDragging)return;
      _spDragging=false;
      panel.style.transition=''; // возвращаем CSS-анимацию
      const dy=Math.max(0,_spDragCurrentY-_spDragStartY);
      const panelH=panel.offsetHeight||360;
      if(dy>panelH*0.28){
        // Достаточно далеко → закрываем
        panel.style.transform=''; // closeStickerPanel сам поставит translateY(100%)
        closeStickerPanel();
      }else{
        // Возвращаем на место
        panel.style.transform='';
      }
    }

    dragZones.forEach(zone=>{
      // Touch
      zone.addEventListener('touchstart',e=>{
        // Не перехватываем если тап на таб (переключение пака)
        if(e.target.closest('.sticker-pack-tab'))return;
        _spDragStart(e.touches[0].clientY);
      },{passive:true});
      zone.addEventListener('touchmove',e=>{
        if(!_spDragging)return;
        e.stopPropagation();
        _spDragMove(e.touches[0].clientY);
      },{passive:true});
      zone.addEventListener('touchend',_spDragEnd,{passive:true});
      // Mouse (desktop)
      zone.addEventListener('mousedown',e=>{
        if(e.target.closest('.sticker-pack-tab,.sticker-panel-close'))return;
        _spDragStart(e.clientY);
        const mm=ev=>_spDragMove(ev.clientY);
        const mu=()=>{_spDragEnd();document.removeEventListener('mousemove',mm);document.removeEventListener('mouseup',mu);};
        document.addEventListener('mousemove',mm);
        document.addEventListener('mouseup',mu);
      });
    });
  }
  // ── Отправляем статус "выбирает стикер..." собеседнику ──
  _startStickerActivity();
  setTimeout(()=>{document.addEventListener('click',_stickerOutsideClick);},80);
}

function _stickerOutsideClick(e){
  const panel=$('stickerPanel');
  if(!panel)return;
  if(!panel.contains(e.target)){
    const ab=document.querySelector('.attach-btn');
    if(ab&&ab.contains(e.target))return;
    const pb=$('pinnedBar');
    if(pb&&pb.contains(e.target))return;
    closeStickerPanel();
  }
}

function closeStickerPanel(){
  const panel=$('stickerPanel');
  if(!panel)return Promise.resolve();
  if(!panel.classList.contains('open'))return Promise.resolve();
  panel.style.transition=''; // восстанавливаем CSS-переход если был drag
  panel.style.transform='';  // убираем inline transform от drag — CSS возьмёт управление
  panel.classList.remove('open');
  const inputBar=$('chatInputBar');
  if(inputBar)inputBar.classList.remove('sticker-mode');
  document.removeEventListener('click',_stickerOutsideClick);
  const se=$('stickerSearch');if(se)se.value='';
  stickerSearchQuery='';
  // ── Останавливаем статус "выбирает стикер..." ──
  _stopStickerActivity();
  // Возвращаем Promise который резолвится после окончания CSS-анимации (340ms)
  return new Promise(res=>setTimeout(res,360));
}

function filterStickers(){
  const q=($('stickerSearch')?.value||'').trim();
  stickerSearchQuery=q;
  renderStickerGrid(stickerActivePack,q);
}

function renderStickerTabs(){
  const wrap=$('stickerTabScroll');
  if(!wrap)return;
  wrap.innerHTML='';
  const rec=getRecentStickers();
  STICKER_PACKS.forEach((pack,idx)=>{
    // Пропускаем "Недавние" если там пусто
    if(pack.id==='recent'&&rec.length===0)return;
    const tab=document.createElement('div');
    tab.className='sticker-pack-tab'+(idx===stickerActivePack?' active':'');
    tab.innerHTML=pack.icon;
    tab.title=pack.name;
    tab.setAttribute('title',pack.name);
    tab.onclick=(e)=>{
      e.stopPropagation();
      stickerActivePack=idx;
      stickerSearchQuery='';
      const se=$('stickerSearch');if(se){se.value='';se.blur();}
      renderStickerTabs();
      renderStickerGrid(idx,'');
    };
    wrap.appendChild(tab);
  });
}

function renderStickerGrid(packIdx,query){
  const grid=$('stickerGrid');
  const nameEl=$('stickerPackName');
  const gridWrap=$('stickerGridWrap');
  if(!grid||!gridWrap)return;

  // Всегда держим #stickerGrid в DOM — только очищаем содержимое.
  // Убираем старый empty-блок если есть, восстанавливаем grid.
  const oldEmpty=gridWrap.querySelector('.sticker-empty');
  if(oldEmpty)oldEmpty.remove();
  if(!gridWrap.contains(grid))gridWrap.appendChild(grid);
  grid.innerHTML='';

  let stickers=[];
  let title='';
  const q=(query||'').trim().toLowerCase();

  if(q){
    stickers=searchStickers(q);
    const count=stickers.length;
    if(!count){
      if(nameEl)nameEl.innerHTML='';
      const empty=document.createElement('div');
      empty.className='sticker-empty';
      empty.innerHTML=`<div class="sticker-empty-icon"><svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="m21 21-4.34-4.34" /> <circle cx="11" cy="11" r="8" /> </svg></div><div>Ничего не найдено по запросу<br><strong style="color:var(--g)">"${q}"</strong><br><span style="opacity:.6;font-size:11px">Попробуй: вода, объятия, самолёт, огонь</span></div>`;
      // Вставляем empty ПЕРЕД grid (grid остаётся в DOM, просто пустой)
      gridWrap.insertBefore(empty,grid);
      return;
    }
    title=`РЕЗУЛЬТАТЫ ПОИСКА <span class="sticker-pack-name-count">${count}</span>`;
  }else{
    const pack=STICKER_PACKS[packIdx]||STICKER_PACKS[1];
    if(pack.id==='recent') pack.stickers=getRecentStickers();
    stickers=pack.stickers;
    title=pack.name.toUpperCase();
  }

  if(nameEl)nameEl.innerHTML=title+(stickers.length?' <span style="opacity:.42;font-weight:400">· '+stickers.length+'</span>':'');

  if(!stickers.length){
    const empty=document.createElement('div');
    empty.className='sticker-empty';
    empty.innerHTML=`<div class="sticker-empty-icon">${STICKER_PACKS[packIdx]?.icon||'<svg class="lucide-icon lucide" xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" > <path d="M21 9a2.4 2.4 0 0 0-.706-1.706l-3.588-3.588A2.4 2.4 0 0 0 15 3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2z" /> <path d="M15 3v5a1 1 0 0 0 1 1h5" /> <path d="M8 13h.01" /> <path d="M16 13h.01" /> <path d="M10 16s.8 1 2 1c1.3 0 2-1 2-1" /> </svg>'}</div><div>Здесь пусто</div>`;
    gridWrap.insertBefore(empty,grid);
    return;
  }

  stickers.forEach((emoji,i)=>{
    const cell=document.createElement('div');
    cell.className='sticker-cell';
    renderStickerVisual(cell,emoji);
    const names=STICKER_NAMES[emoji];
    if(names&&names.length) cell.title=names[0];
    if(i<30){
      cell.style.animationDelay=(i*12)+'ms';
      cell.classList.add('new-appear');
    }
    cell.onclick=(e)=>{
      e.stopPropagation();
      cell.classList.add('sending');
      setTimeout(()=>cell.classList.remove('sending'),360);
      sendSticker(emoji);
    };
    grid.appendChild(cell);
  });
}

async function sendSticker(emoji){
  if(!activePid||!wsUp){toast('Нет соединения','err');return;}
  if(isContactBlocked(activePid))return;
  _vib('impactLight'); // тактильный отклик — стикер отправлен
  // Панель НЕ закрываем — пользователь может отправить ещё стикеры
  const key=await getKey(activePid);
  if(!key){toast('Сначала установите ключ шифрования','warn');return;}
  // Запоминаем в недавние
  addRecentSticker(emoji);
  const msgId=uid();
  const now=new Date().toISOString();
  const emojiName=STICKER_NAMES[emoji]?STICKER_NAMES[emoji][0]:'стикер';
  const envelope={type:'sticker',id:msgId,sticker:emoji,ts:Date.now(),replyTo:replyTo||null};
  const localMsg={id:msgId,sticker:emoji,type:'sent',time:now,reactions:{},edited:false,replyTo:replyTo||null,delivered:false,forwarded:false};
  cancelReply();
  await upsertMsg(activePid,localMsg);
  await updateChat(activePid,{lastMsg:`${emoji} Стикер`,lastMsgTime:Date.now()});
  appendOrReloadMsg(activePid,localMsg);
  renderChatList();
  try{
    const enc=await encData(ENC.encode(JSON.stringify(envelope)),key);
    wsSend({type:'send-msg',target:activePid,msgId,payload:payloadToB64(enc)});
  }catch(e){toast('Ошибка отправки стикера','err');}
}
// ── КОНЕЦ СТИКЕРОВ ──


// ══════════════════════════════════════════════════════
// ── БИОМЕТРИЧЕСКАЯ БЛОКИРОВКА (Median.co Callback API) ──
// ══════════════════════════════════════════════════════

var _bioLocked   = false;
var _bioUnlocked = false;
var _bioTimer    = null;

// Показать экран блокировки и запустить запрос биометрии
function bioLock(){
  if(_bioLocked) return;
  _bioLocked   = true;
  _bioUnlocked = false;
  var scr = document.getElementById('bioLockScreen');
  if(scr) scr.classList.add('visible');
  clearTimeout(_bioTimer);
  _bioTimer = setTimeout(function(){ window.bioPrompt(); }, 400);
}

// Скрыть экран блокировки
function bioUnlock(){
  _bioLocked   = false;
  _bioUnlocked = true;
  var scr = document.getElementById('bioLockScreen');
  if(scr) scr.classList.remove('visible');
  var errEl = document.getElementById('bioLockErr');
  if(errEl) errEl.textContent = '';
}

// Запрос биометрии — только callback-стиль (работает на iOS + Android)
window.bioPrompt = function(){
  // Не в Median — просто пускаем
  if(typeof median === 'undefined' || !median || !median.auth || !median.auth.get){
    bioUnlock(); return;
  }
  var btn   = document.getElementById('bioLockBtn');
  var errEl = document.getElementById('bioLockErr');
  if(btn)   btn.classList.add('pulse');
  if(errEl) errEl.textContent = '';

  // ВАЖНО: вызов native-биометрии (median.auth.get) переводит WebView
  // в background на время показа системного диалога Touch/Face ID —
  // это вызывает document.visibilitychange ('hidden' → 'visible').
  // Без этого флага обработчик visibilitychange (см. ниже) решал, что
  // приложение было свёрнуто пользователем (>3с), и вызывал
  // goHome()/leaveChat() — что сбрасывало экран выбора чата,
  // открытый через "Поделиться" (median_share_to_app), ДО того как
  // пользователь успевал отправить сообщение. Помечаем этот переход
  // как "ожидаемый", чтобы visibilitychange его проигнорировал.
  ignoreNextVisibilityReturn = true;

  median.auth.get({
    prompt:           'Войти в K-Chat',
    callbackFunction: function(result){
      if(btn) btn.classList.remove('pulse');
      if(result && result.success && result.secret === 'kchat_bio_unlock'){
        bioUnlock();
      } else {
        var code = result && result.error;
        var msg  = 'Ошибка. Нажмите для повтора.';
        if(code === 'userCanceled')        msg = 'Отменено. Нажмите для повтора.';
        if(code === 'authenticationFailed') msg = 'Не распознано. Попробуйте ещё раз.';
        if(code === 'itemNotFound'){
          // Секрет потерян (переустановка?) — сбрасываем настройку
          lsSet('bc_bio_lock', false);
          bioUnlock();
          return;
        }
        if(errEl) errEl.textContent = msg;
        // Повторный показ диалога биометрии (после повторного нажатия)
        // также может вызвать visibilitychange — снова игнорируем его.
        ignoreNextVisibilityReturn = true;
      }
    },
    callbackOnCancel: 1
  });
};

// Инициализация при старте — проверяем нужна ли блокировка
function bioInit(){
  if(!lsGet('bc_bio_lock', false)) return;
  if(typeof median === 'undefined' || !median || !median.auth || !median.auth.status) return;

  // Используем только callback (не Promise) для максимальной совместимости
  median.auth.status({callbackFunction: function(data){
    if(data && data.hasTouchId && data.hasSecret){
      bioLock();
    } else {
      // Биометрия недоступна или секрет удалён — сбрасываем
      if(!data || !data.hasTouchId){
        lsSet('bc_bio_lock', false);
      }
    }
  }});
}

// Блокируем при сворачивании приложения
document.addEventListener('visibilitychange', function(){
  if(!lsGet('bc_bio_lock', false)) return;
  if(document.visibilityState === 'hidden'){
    _bioUnlocked = false;
  } else if(document.visibilityState === 'visible'){
    if(!_bioUnlocked && !_bioLocked) bioLock();
  }
});

// Median callback при возврате в приложение (Android)
window.gonative_app_resumed = function(){
  if(lsGet('bc_bio_lock', false) && !_bioUnlocked) bioLock();
};

// Запускаем с небольшой задержкой (Median API должен загрузиться)
setTimeout(function(){ bioInit(); }, 600);

})();
