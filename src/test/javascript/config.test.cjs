const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync, existsSync } = require('node:fs');
const { join } = require('node:path');
const vm = require('node:vm');
const source = readFileSync(join(__dirname, '../../main/resources/org/jenkinsci/plugins/rabbitmqconsumer/GlobalRabbitmqConfiguration/connection-status.js'), 'utf8');

test('status adjunct does not resolve the global configuration Jelly as an inclusion fragment', () => {
  const resources = join(__dirname, '../../main/resources');
  const jelly = readFileSync(join(resources, 'org/jenkinsci/plugins/rabbitmqconsumer/GlobalRabbitmqConfiguration/config.jelly'), 'utf8');
  const adjunct = jelly.match(/<st:adjunct includes="([^"]+)"/)[1].replace(/\./g, '/');
  assert.equal(existsSync(join(resources, adjunct + '.js')), true);
  assert.equal(existsSync(join(resources, adjunct + '.jelly')), false);
});

function page({ behaviour = true, loading = false, present = true, proxy = true } = {}) {
  const status = { dataset: {
    connected: 'connesso <ok>', disconnected: 'disconnesso',
    connectedIcon: '/jenkins/static/test/images/16x16/blue.png',
    disconnectedIcon: '/jenkins/static/test/images/16x16/red.png',
  }, textContent: '', isConnected: present, closest() { return null; } };
  Object.defineProperty(status, 'innerHTML', {
    set() { throw new Error('HTML assignment is not allowed'); },
  });
  const image = { src: '', isConnected: present };
  const elements = present ? { 'rabbitmq-status-text': status, 'rabbitmq-status-img': image } : {};
  status.parentNode = { querySelector() { return elements['rabbitmq-status-img'] || null; } };
  const hiddenImage = { src: '', isConnected: true };
  const hiddenStatus = {
    dataset: { ...status.dataset, connectedIcon: '/16x16/blue.png', disconnectedIcon: '/16x16/red.png' },
    textContent: '', isConnected: true,
    closest(selector) { return selector === '.jenkins-hidden' ? {} : null; },
    parentNode: { querySelector() { return hiddenImage; } },
  };
  const callbacks = [], timers = [], listeners = {};
  let initializer;
  const descriptor = { isOpen(callback) { callbacks.push(callback); } };
  const window = { setInterval(callback, delay) { timers.push({ callback, delay }); } };
  const context = vm.createContext({
    window,
    binding: descriptor,
    document: {
      readyState: loading ? 'loading' : 'complete',
      // The hidden duplicate is first, reproducing the rendered Jenkins HTML.
      getElementById(id) { return id === 'rabbitmq-status-text' ? hiddenStatus : hiddenImage; },
      querySelectorAll() { return present ? [hiddenStatus, status] : [hiddenStatus]; },
      addEventListener(name, callback) { listeners[name] = callback; },
    },
  }, { codeGeneration: { strings: false, wasm: false } });
  function installBinding() {
    // Standalone binding with global lexical scope, deliberately absent from window.
    vm.runInContext('const rabbitmqConsumerCspDescriptor = binding;', context);
  }
  if (proxy) installBinding();
  if (behaviour) context.Behaviour = {
    specify(selector, name, priority, callback) {
      assert.equal(selector, '#rabbitmq-status-text');
      initializer = callback;
    },
  };
  vm.runInContext(source, context);
  return { status, image, hiddenStatus, hiddenImage, elements, callbacks, timers, listeners, window, descriptor,
    installBinding,
    apply(element = status) { initializer(element); },
    respond(value) { callbacks.shift()({ responseObject() { return value; } }); },
  };
}

test('lexical binding and lazy configuration update the real row, skip hidden duplicates and poll every 15 seconds once', () => {
  const p = page({ present: false });
  assert.equal(p.callbacks.length, 0);
  p.apply(p.hiddenStatus);
  assert.equal(p.timers.length, 0);
  p.status.isConnected = p.image.isConnected = true;
  p.elements['rabbitmq-status-text'] = p.status;
  p.elements['rabbitmq-status-img'] = p.image;
  p.apply();
  assert.equal(p.callbacks.length, 1);
  p.respond(true);
  assert.equal(p.status.textContent, 'connesso <ok>');
  assert.equal(p.image.src, p.status.dataset.connectedIcon);
  assert.equal(p.hiddenStatus.textContent, '');
  assert.equal(p.hiddenImage.src, '');
  assert.equal(p.window.rabbitmqConsumerCspDescriptor, undefined);
  assert.equal(p.timers.length, 1);
  assert.equal(p.timers[0].delay, 15000);
  p.timers[0].callback();
  p.respond(false);
  assert.equal(p.status.textContent, 'disconnesso');
  assert.equal(p.image.src, p.status.dataset.disconnectedIcon);
  p.apply();
  assert.equal(p.timers.length, 1);
  assert.equal(p.callbacks.length, 0);
});

test('removed elements and late responses do not throw or send needless requests', () => {
  const p = page();
  p.apply();
  delete p.elements['rabbitmq-status-img'];
  assert.doesNotThrow(() => p.respond(true));
  assert.doesNotThrow(() => p.timers[0].callback());
  assert.equal(p.callbacks.length, 0);
  assert.equal(p.status.textContent, '');
  p.elements['rabbitmq-status-img'] = p.image;
  p.status.isConnected = false;
  assert.doesNotThrow(() => p.timers[0].callback());
  assert.equal(p.callbacks.length, 0);
});

test('missing binding and null Behaviour elements are safe and remain retryable', () => {
  const p = page({ proxy: false });
  assert.doesNotThrow(() => p.apply(null));
  assert.doesNotThrow(() => p.apply());
  assert.equal(p.timers.length, 0);
  p.installBinding();
  p.apply();
  assert.equal(p.callbacks.length, 1);
});

test('DOMContentLoaded fallback waits for markup; already-loaded markup initializes immediately', () => {
  const waiting = page({ behaviour: false, loading: true });
  assert.equal(waiting.callbacks.length, 0);
  waiting.listeners.DOMContentLoaded();
  assert.equal(waiting.callbacks.length, 1);
  const ready = page({ behaviour: false });
  assert.equal(ready.callbacks.length, 1);
  assert.equal(ready.timers[0].delay, 15000);
});

test('unexpected proxy responses do not replace status or throw', () => {
  const p = page();
  p.apply();
  assert.doesNotThrow(() => p.callbacks.shift()(null));
  p.timers[0].callback();
  assert.doesNotThrow(() => p.callbacks.shift()({}));
  assert.equal(p.status.textContent, '');
});
