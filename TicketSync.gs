/**
 * TicketSync - Teamwork Desk → Notion ticket integration.
 * Single-file implementation with modular sections:
 *  - Config: reads & validates Script Properties
 *  - TeamworkDeskClient: Desk API helpers with backoff + pagination
 *  - NotionClient: Notion REST helpers for databases + pages
 *  - Mappers: translate Desk ticket to Notion properties
 *  - SyncEngine: orchestration for webhooks, polling, backfill, merges, deletions
 *  - Endpoints: doPost (webhooks), doGet (health), pollDeskForChanges, backfillLastNDays
 */

/* ============================= Config ============================= */

// Optional: hard-code the Clients DB ID for troubleshooting to bypass Script Properties.
// Leave blank to use the value from Script Properties.
var CLIENTS_DB_ID_OVERRIDE = "2bef5364-d1df-8001-9ed9-e137d9409b1a";

function getConfig_() {
  const props = PropertiesService.getScriptProperties();
  const required = [
    "TEAMWORK_SITE",
    "TEAMWORK_DESK_API_KEY",
    "NOTION_TOKEN",
    "NOTION_VERSION",
    "NOTION_DB_TICKETS",
    "NOTION_CLIENTS_DB_ID",
    "NOTION_CONTACTS_DB_ID",
    "NOTION_DB_TEAM_DIRECTORY",
    "NOTION_DB_PROJECTS",
    "SYNC_INBOX_NAME_OR_ID",
    "BACKFILL_DAYS",
    "POLLING_ENABLED",
    "POLLING_INTERVAL_MINUTES"
  ];

  const cfg = {};
  required.forEach(function (k) {
    const v = props.getProperty(k);
    if (v === null || v === undefined || v === "") {
      throw new Error("Missing Script Property: " + k);
    }
    cfg[k] = typeof v === "string" ? v.trim() : v;
  });

  if (CLIENTS_DB_ID_OVERRIDE) {
    cfg.NOTION_CLIENTS_DB_ID = CLIENTS_DB_ID_OVERRIDE.trim();
  }

  cfg.BACKFILL_DAYS = Number(cfg.BACKFILL_DAYS);
  cfg.POLLING_ENABLED = String(cfg.POLLING_ENABLED).toLowerCase() === "true";
  cfg.POLLING_INTERVAL_MINUTES = Number(cfg.POLLING_INTERVAL_MINUTES);
  cfg.TEAMWORK_DESK_WEBHOOK_SECRET = props.getProperty("TEAMWORK_DESK_WEBHOOK_SECRET") || "";
  cfg.RATE_LIMIT_RETRIES = 5;
  cfg.RATE_LIMIT_BASE_BACKOFF_MS = 750;
  cfg.IDEMPOTENCY_CACHE_SECONDS = 60 * 60; // 1 hour
  cfg.LAST_POLL_PROP = "LAST_DESK_TICKET_POLL_ISO";

  return cfg;
}

/* ===================== HTTP + Backoff Utilities ===================== */

function fetchWithBackoff_(url, options, retries, baseBackoffMs) {
  var attempt = 0;
  var max = 1 + (retries || 0);
  var base = baseBackoffMs || 500;

  while (attempt < max) {
    attempt++;
    var res = UrlFetchApp.fetch(url, options);
    var code = res.getResponseCode();
    var body = res.getContentText();

    if (code >= 200 && code < 300) {
      try {
        return body ? JSON.parse(body) : {};
      } catch (err) {
        return {};
      }
    }

    var retryable = (code === 429) || (code >= 500 && code <= 599);
    if (!retryable || attempt >= max) {
      throw new Error("HTTP " + code + " " + options.method + " " + url + " :: " + body);
    }

    var sleep = base * Math.pow(2, attempt - 1);
    Utilities.sleep(sleep);
  }

  throw new Error("Exhausted retries for " + url);
}

function buildHeaders_(extra) {
  var h = { "Content-Type": "application/json" };
  extra = extra || {};
  for (var k in extra) h[k] = extra[k];
  return h;
}

/* ========================= Teamwork Desk Client ========================= */

function TeamworkDeskClient_(cfg) {
  var base = "https://" + cfg.TEAMWORK_SITE + "/desk/api/v2";
  var headers = buildHeaders_({ Authorization: "Bearer " + cfg.TEAMWORK_DESK_API_KEY });

  function buildUrl_(path, params) {
    var qs = [];
    for (var k in params) {
      if (params[k] === null || params[k] === undefined || params[k] === "") continue;
      if (Array.isArray(params[k])) {
        params[k].forEach(function (v) {
          qs.push(encodeURIComponent(k) + "=" + encodeURIComponent(String(v)));
        });
      } else {
        qs.push(encodeURIComponent(k) + "=" + encodeURIComponent(String(params[k])));
      }
    }
    return base + path + (qs.length ? ("?" + qs.join("&")) : "");
  }

  function listTickets_(opts) {
    var params = {
      inboxId: opts.inboxId || undefined,
      updatedAtFrom: opts.updatedSince || undefined,
      page: opts.page || 1,
      pageSize: opts.pageSize || 100,
      spam: false,
      sortBy: "updatedAt",
      orderBy: "asc"
    };
    var url = buildUrl_("/tickets.json", params);
    return fetchWithBackoff_(url, { method: "get", headers: headers, muteHttpExceptions: true }, cfg.RATE_LIMIT_RETRIES, cfg.RATE_LIMIT_BASE_BACKOFF_MS);
  }

  function getTicket_(ticketId) {
    var url = base + "/tickets/" + encodeURIComponent(String(ticketId)) + ".json";
    return fetchWithBackoff_(url, { method: "get", headers: headers, muteHttpExceptions: true }, cfg.RATE_LIMIT_RETRIES, cfg.RATE_LIMIT_BASE_BACKOFF_MS);
  }

  function getTicketTasks_(ticketId) {
    var url = base + "/tickets/" + encodeURIComponent(String(ticketId)) + "/tasks.json";
    return fetchWithBackoff_(url, { method: "get", headers: headers, muteHttpExceptions: true }, cfg.RATE_LIMIT_RETRIES, cfg.RATE_LIMIT_BASE_BACKOFF_MS);
  }

  return {
    listTickets: listTickets_,
    getTicket: getTicket_,
    getTicketTasks: getTicketTasks_
  };
}

/* ============================ Notion Client ============================ */

function NotionClient_(cfg) {
  var headers = buildHeaders_({
    Authorization: "Bearer " + cfg.NOTION_TOKEN,
    "Notion-Version": cfg.NOTION_VERSION
  });

  function queryByNumber_(dbId, property, numberValue) {
    var body = {
      page_size: 1,
      filter: {
        property: property,
        number: { equals: Number(numberValue) }
      }
    };
    return fetchWithBackoff_("https://api.notion.com/v1/databases/" + dbId + "/query", {
      method: "post",
      headers: headers,
      payload: JSON.stringify(body),
      muteHttpExceptions: true
    }, 3, 800);
  }

  function queryBySelect_(dbId, property, value) {
    var body = {
      page_size: 1,
      filter: {
        property: property,
        select: { equals: value }
      }
    };
    return fetchWithBackoff_("https://api.notion.com/v1/databases/" + dbId + "/query", {
      method: "post",
      headers: headers,
      payload: JSON.stringify(body),
      muteHttpExceptions: true
    }, 3, 800);
  }

  function createPage_(dbId, properties) {
    return fetchWithBackoff_("https://api.notion.com/v1/pages", {
      method: "post",
      headers: headers,
      payload: JSON.stringify({ parent: { database_id: dbId }, properties: properties }),
      muteHttpExceptions: true
    }, 3, 800);
  }

  function updatePage_(pageId, properties) {
    return fetchWithBackoff_("https://api.notion.com/v1/pages/" + pageId, {
      method: "patch",
      headers: headers,
      payload: JSON.stringify({ properties: properties }),
      muteHttpExceptions: true
    }, 3, 800);
  }

  function archivePage_(pageId) {
    return fetchWithBackoff_("https://api.notion.com/v1/pages/" + pageId, {
      method: "patch",
      headers: headers,
      payload: JSON.stringify({ archived: true }),
      muteHttpExceptions: true
    }, 3, 800);
  }

  return {
    queryByNumber: queryByNumber_,
    queryBySelect: queryBySelect_,
    createPage: createPage_,
    updatePage: updatePage_,
    archivePage: archivePage_
  };
}

/* ============================== Mappers ============================== */

function mapDeskTicketToNotionProps_(ticket, related, cfg) {
  var props = {};
  var needsReview = false;

  props["Ticket ID"] = { number: Number(ticket.id) };
  props["Subject"] = { title: [{ type: "text", text: { content: ticket.subject || "(No subject)" } }] };
  props["Status"] = ticket.status ? { select: { name: ticket.status } } : { select: null };
  props["Priority"] = ticket.priority ? { select: { name: ticket.priority } } : { select: null };
  props["Tags"] = { multi_select: (ticket.tags || []).map(function (t) { return { name: t }; }) };
  props["Ticket Link"] = ticket.url ? { url: ticket.url } : { url: null };
  props["Date Created"] = ticket.createdAt ? { date: { start: ticket.createdAt } } : { date: null };

  if (related.clientId) {
    props["Client"] = { relation: [{ id: related.clientId }] };
  } else {
    props["Client"] = { relation: [] };
    needsReview = true;
  }

  if (related.contactId) {
    props["Contact"] = { relation: [{ id: related.contactId }] };
  } else {
    props["Contact"] = { relation: [] };
    needsReview = true;
  }

  if (related.assigneeId) {
    props["Assignee"] = { relation: [{ id: related.assigneeId }] };
  } else {
    props["Assignee"] = { relation: [] };
  }

  if (related.projectIds && related.projectIds.length) {
    props["Project"] = { relation: related.projectIds.map(function (id) { return { id: id }; }) };
  } else {
    props["Project"] = { relation: [] };
  }

  if (needsReview) {
    props["Review"] = { select: { name: "Needs review" } };
  } else {
    props["Review"] = { select: { name: "Complete" } };
  }

  return props;
}

/* ============================= Sync Engine ============================= */

function SyncEngine_(cfg, desk, notion) {
  var cache = CacheService.getScriptCache();

  function isSupportInbox_(ticket) {
    var inbox = ticket.inbox || {};
    var target = String(cfg.SYNC_INBOX_NAME_OR_ID).trim().toLowerCase();
    var name = (inbox.name || "").trim().toLowerCase();
    var id = inbox.id !== undefined && inbox.id !== null ? String(inbox.id).trim().toLowerCase() : "";
    return target && (name === target || id === target);
  }

  function isSpam_(ticket) {
    return String(ticket.spam || "").toLowerCase() === "true" || ticket.spam === true;
  }

  function ensureTicketAllowed_(ticket) {
    if (!isSupportInbox_(ticket)) return false;
    if (isSpam_(ticket)) return false;
    return true;
  }

  function resolveClient_(companyId) {
    if (!companyId) return null;
    var res = notion.queryByNumber(cfg.NOTION_CLIENTS_DB_ID, "Desk Company ID", companyId);
    if (res.results && res.results.length) return res.results[0].id;
    return null;
  }

  function normalizeEmail_(email) {
    if (!email) return "";
    return String(email).trim().toLowerCase();
  }

  function resolveContact_(ticket) {
    var contact = ticket.customer || ticket.contact || {};
    if (contact.id) {
      var byId = notion.queryByNumber(cfg.NOTION_CONTACTS_DB_ID, "Desk Contact ID", contact.id);
      if (byId.results && byId.results.length) return byId.results[0].id;
    }

    var emails = [];
    if (contact.email) emails.push(contact.email);
    if (contact.emails && contact.emails.length) emails = emails.concat(contact.emails);
    if (ticket.customerEmail) emails.push(ticket.customerEmail);

    var dedup = {};
    emails.forEach(function (e) {
      var v = normalizeEmail_(e);
      if (v) dedup[v] = true;
    });

    var emailKeys = Object.keys(dedup);
    for (var i = 0; i < emailKeys.length; i++) {
      var email = emailKeys[i];
      var checks = ["Email", "Secondary Email", "Email 3", "Email 4"];
      for (var j = 0; j < checks.length; j++) {
        var res = fetchWithBackoff_("https://api.notion.com/v1/databases/" + cfg.NOTION_CONTACTS_DB_ID + "/query", {
          method: "post",
          headers: buildHeaders_({
            Authorization: "Bearer " + cfg.NOTION_TOKEN,
            "Notion-Version": cfg.NOTION_VERSION
          }),
          payload: JSON.stringify({
            page_size: 1,
            filter: { property: checks[j], email: { equals: email } }
          }),
          muteHttpExceptions: true
        }, 3, 800);
        if (res.results && res.results.length) return res.results[0].id;
      }
    }

    if (contact.name) {
      var resName = fetchWithBackoff_("https://api.notion.com/v1/databases/" + cfg.NOTION_CONTACTS_DB_ID + "/query", {
        method: "post",
        headers: buildHeaders_({
          Authorization: "Bearer " + cfg.NOTION_TOKEN,
          "Notion-Version": cfg.NOTION_VERSION
        }),
        payload: JSON.stringify({
          page_size: 1,
          filter: { property: "Contact Name", title: { equals: contact.name } }
        }),
        muteHttpExceptions: true
      }, 3, 800);
      if (resName.results && resName.results.length) return resName.results[0].id;
    }

    return null;
  }

  function resolveAssignee_(ticket) {
    var assignee = ticket.assignee || ticket.assignedTo;
    if (!assignee || !assignee.id) return null;
    var res = notion.queryByNumber(cfg.NOTION_DB_TEAM_DIRECTORY, "Desk Agent ID", assignee.id);
    if (res.results && res.results.length) return res.results[0].id;
    return null;
  }

  function resolveProjects_(ticket) {
    var ids = [];
    if (ticket.project && ticket.project.id) ids.push(ticket.project.id);

    if (!ids.length) {
      var tasks = desk.getTicketTasks(ticket.id);
      var included = (tasks.tasks || tasks.data || []);
      for (var i = 0; i < included.length; i++) {
        var t = included[i];
        if (t.projectId && ids.indexOf(t.projectId) === -1) ids.push(t.projectId);
      }
    }

    var notionIds = [];
    ids.forEach(function (pid) {
      var res = notion.queryByNumber(cfg.NOTION_DB_PROJECTS, "Project ID", pid);
      if (res.results && res.results.length) {
        notionIds.push(res.results[0].id);
      }
    });
    return notionIds;
  }

  function findTicketPage_(ticketId) {
    var res = notion.queryByNumber(cfg.NOTION_DB_TICKETS, "Ticket ID", ticketId);
    if (res.results && res.results.length) return res.results[0];
    return null;
  }

  function upsertTicket_(ticket) {
    if (!ensureTicketAllowed_(ticket)) {
      console.log(JSON.stringify({ level: "info", message: "Ticket ignored (not support inbox or spam)", ticketId: ticket.id }));
      return;
    }

    var related = {
      clientId: resolveClient_(ticket.company && ticket.company.id),
      contactId: resolveContact_(ticket),
      assigneeId: resolveAssignee_(ticket),
      projectIds: resolveProjects_(ticket)
    };

    var properties = mapDeskTicketToNotionProps_(ticket, related, cfg);
    var existing = findTicketPage_(ticket.id);

    if (existing) {
      notion.updatePage(existing.id, properties);
      console.log(JSON.stringify({ level: "info", message: "Ticket updated in Notion", ticketId: ticket.id, pageId: existing.id }));
    } else {
      var created = notion.createPage(cfg.NOTION_DB_TICKETS, properties);
      console.log(JSON.stringify({ level: "info", message: "Ticket created in Notion", ticketId: ticket.id, pageId: created.id }));
    }
  }

  function deleteTicket_(ticketId) {
    var existing = findTicketPage_(ticketId);
    if (!existing) return;
    notion.archivePage(existing.id);
    console.log(JSON.stringify({ level: "info", message: "Ticket archived in Notion", ticketId: ticketId, pageId: existing.id }));
  }

  function handleMerge_(payload) {
    var mergedTo = payload.mergedToTicketId || payload.merged_to_ticket_id;
    var fromId = payload.id || payload.ticketId;
    if (fromId) deleteTicket_(fromId);
    if (mergedTo) {
      var full = desk.getTicket(mergedTo);
      var ticket = full.ticket || full.data || full;
      upsertTicket_(ticket);
    }
  }

  function handleWebhookEvent_(body) {
    var event = body.event || body.type || "";
    var payload = body.ticket || body.payload || body.data || body;
    var ticketId = payload.id || payload.ticketId;

    if (event === "ticket.deleted") {
      deleteTicket_(ticketId);
      return;
    }
    if (event === "ticket.merged") {
      handleMerge_(payload);
      return;
    }

    var full = desk.getTicket(ticketId);
    var ticket = full.ticket || full.data || full;
    upsertTicket_(ticket);
  }

  function handleWebhook_(e) {
    var raw = e.postData && e.postData.contents ? e.postData.contents : "";
    var headers = e && e.headers ? e.headers : {};
    var deliveryId = headers["X-Desk-Delivery"] || headers["x-desk-delivery"] || "";
    if (deliveryId) {
      var key = "desk-delivery-" + deliveryId;
      var seen = cache.get(key);
      if (seen) {
        console.log(JSON.stringify({ level: "info", message: "Duplicate delivery ignored", deliveryId: deliveryId }));
        return ContentService.createTextOutput("duplicate");
      }
      cache.put(key, "1", cfg.IDEMPOTENCY_CACHE_SECONDS);
    }

    if (cfg.TEAMWORK_DESK_WEBHOOK_SECRET) {
      var sig = headers["X-Desk-Signature"] || headers["x-desk-signature"];
      var expected = Utilities.computeHmacSha256Signature(raw, cfg.TEAMWORK_DESK_WEBHOOK_SECRET);
      var expectedHex = expected.map(function (b) {
        var s = (b & 0xff).toString(16);
        return s.length === 1 ? "0" + s : s;
      }).join("");
      if (!sig || sig.trim().toLowerCase() !== expectedHex.toLowerCase()) {
        console.log(JSON.stringify({ level: "error", message: "Invalid webhook signature" }));
        return ContentService.createTextOutput("invalid signature").setResponseCode(401);
      }
    }

    var body = {};
    if (raw) {
      try {
        body = JSON.parse(raw);
      } catch (err) {
        console.log(JSON.stringify({ level: "error", message: "Invalid JSON payload", error: String(err) }));
        return ContentService.createTextOutput("bad json").setResponseCode(400);
      }
    }

    handleWebhookEvent_(body);
    return ContentService.createTextOutput("ok");
  }

  function pollChanges_() {
    if (!cfg.POLLING_ENABLED) {
      console.log(JSON.stringify({ level: "info", message: "Polling disabled" }));
      return;
    }
    var props = PropertiesService.getScriptProperties();
    var since = props.getProperty(cfg.LAST_POLL_PROP);
    var page = 1;
    var newest = since;
    var hasMore = true;

    while (hasMore) {
      var res = desk.listTickets({ inboxId: cfg.SYNC_INBOX_NAME_OR_ID, updatedSince: since, page: page, pageSize: 100 });
      var tickets = res.tickets || res.data || [];
      tickets.forEach(function (t) {
        upsertTicket_(t);
        if (!newest || t.updatedAt > newest) newest = t.updatedAt;
      });
      hasMore = res.has_more || res.hasMore || (tickets.length === 100);
      page++;
    }

    if (newest) props.setProperty(cfg.LAST_POLL_PROP, newest);
  }

  function backfillLastNDays_() {
    var days = cfg.BACKFILL_DAYS || 120;
    var sinceDate = new Date();
    sinceDate.setDate(sinceDate.getDate() - days);
    var iso = sinceDate.toISOString();

    var page = 1;
    var hasMore = true;
    while (hasMore) {
      var res = desk.listTickets({ inboxId: cfg.SYNC_INBOX_NAME_OR_ID, updatedSince: iso, page: page, pageSize: 100 });
      var tickets = res.tickets || res.data || [];
      tickets.forEach(function (t) {
        upsertTicket_(t);
      });
      hasMore = res.has_more || res.hasMore || (tickets.length === 100);
      page++;
    }
  }

  return {
    upsertTicket: upsertTicket_,
    deleteTicket: deleteTicket_,
    handleWebhook: handleWebhook_,
    pollChanges: pollChanges_,
    backfillLastNDays: backfillLastNDays_
  };
}

/* ============================= Endpoints ============================= */

function getEngine_() {
  var cfg = getConfig_();
  var desk = TeamworkDeskClient_(cfg);
  var notion = NotionClient_(cfg);
  return SyncEngine_(cfg, desk, notion);
}

function doPost(e) {
  return getEngine_().handleWebhook(e);
}

function doGet() {
  return ContentService.createTextOutput("ok");
}

function pollDeskForChanges() {
  getEngine_().pollChanges();
}

function backfillLastNDays() {
  getEngine_().backfillLastNDays();
}
