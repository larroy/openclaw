/* @vitest-environment jsdom */
import { expect, it } from "vitest";
import { flattenTranslations } from "../../../scripts/lib/control-ui-i18n-sync-plan.ts";
import { useLazyEnglishTest } from "./lazy-english.test-support.ts";

const loadI18n = useLazyEnglishTest();

it("loads optional English copy at each view boundary while keeping shared labels eager", async () => {
  const { en, manager } = await loadI18n();
  const about = en.aboutPage;
  const dashboards = en.dashboardsPage;
  const diff = en.chat.sessionDiff;
  expect(manager.t("aboutPage.artifactDetails")).toBe("Control UI build details");
  expect(manager.t("aboutPage.copiedCommit")).toBe("Commit hash copied");
  expect(manager.t("chat.sessionDiff.title")).toBe("Changes");
  expect(manager.t("chat.sessionDiff.show")).toBe("Show session changes");
  const { registerChatSessionEnglish } = await import("./locales/en-chat-session.ts");
  registerChatSessionEnglish();
  expect(manager.t("chat.sessionDiff.expandFile")).toBe("chat.sessionDiff.expandFile");
  expect(manager.t("chat.sessionDiff.loading")).toBe("chat.sessionDiff.loading");
  await manager.setLocale("de");
  for (const { key, load, catalog } of [
    {
      key: "aboutPage.tagline",
      load: () => import("../pages/about/view.tsx"),
      catalog: async () => (await import("./locales/en-about.ts")).registerAboutEnglish.catalog,
    },
    {
      key: "dashboardsPage.emptyDescription",
      load: () => import("../pages/dashboards/view.tsx"),
      catalog: async () =>
        (await import("./locales/en-dashboards.ts")).registerDashboardsEnglish.catalog,
    },
    {
      key: "chat.sessionDiff.loading",
      load: () => import("../pages/chat/components/session-diff-panel.tsx"),
      catalog: async () =>
        (await import("./locales/en-session-diff.ts")).registerSessionDiffEnglish.catalog,
    },
  ]) {
    expect(manager.t(key)).toBe(key);
    await load();
    for (const [catalogKey, value] of flattenTranslations(await catalog())) {
      expect(manager.t(catalogKey)).toBe(value);
    }
    expect(manager.t("common.health")).toBe("Gesundheit");
    expect(en.aboutPage).toBe(about);
    expect(en.dashboardsPage).toBe(dashboards);
    expect(en.chat.sessionDiff).toBe(diff);
  }
});
