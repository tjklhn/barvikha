import React, { useEffect, useRef, useState } from "react";
import { apiFetchJson } from "../api";
import { XIcon, ChevronRightIcon, RefreshIcon, LoadingIcon, FileIcon } from "./Icons";

const AdModal = ({
  isOpen,
  onClose,
  onSubmit,
  publishing,
  accounts,
  categories,
  onRefreshCategories,
  loadingCategories,
  extraFields,
  extraFieldValues,
  setExtraFieldValues,
  loadingExtraFields,
  extraFieldsError,
  newAd,
  setNewAd,
  adImages,
  setAdImages,
  categoriesUpdatedAt
}) => {
  if (!isOpen) return null;

  const [categoryPath, setCategoryPath] = useState([]);
  const [localCategories, setLocalCategories] = useState([]);
  const [didRestorePath, setDidRestorePath] = useState(false);
  const [imagePreviews, setImagePreviews] = useState([]);
  const [rightDragActive, setRightDragActive] = useState(false);
  const [rightDragIndex, setRightDragIndex] = useState(null);
  const [loadingPlz, setLoadingPlz] = useState(false);
  const [categoryChildrenError, setCategoryChildrenError] = useState("");
  const categoryListRef = useRef(null);
  const imageInputRef = useRef(null);
  const pendingChildrenRef = useRef(new Map());
  const prefetchRootRef = useRef("");
  const prefetchInFlightRef = useRef(false);
  const prefetchedTargetsRef = useRef(new Set());
  const prefetchTokenRef = useRef(0);
  const rightDragRef = useRef({ active: false, index: -1 });

  const inputStyle = {
    width: "100%",
    padding: "12px 16px",
    border: "1px solid rgba(148,163,184,0.2)",
    borderRadius: "14px",
    background: "rgba(15,23,42,0.8)",
    fontSize: "14px",
    color: "#e2e8f0",
    transition: "all 0.2s ease",
    outline: "none"
  };

  const labelStyle = {
    display: "block",
    marginBottom: "10px",
    fontWeight: "600",
    color: "#e2e8f0",
    fontSize: "14px"
  };
  const selectedUnderlineStyle = {
    borderBottom: "1px solid rgba(125, 211, 252, 0.95)",
    paddingBottom: "1px"
  };
  const missingRequiredExtraFields = (Array.isArray(extraFields) ? extraFields : [])
    .filter((field) => field?.required)
    .filter((field) => {
      const key = field.name || field.label;
      const value = key ? extraFieldValues?.[key] : "";
      return !String(value ?? "").trim();
    });
  const hasSelectedCategory = Boolean(newAd?.categoryId || newAd?.categoryUrl);
  const submitBlockedByFields =
    hasSelectedCategory && (loadingExtraFields || Boolean(extraFieldsError) || missingRequiredExtraFields.length > 0);

  const formatAccountLabel = (account) => {
    const name = account.profileName || account.username || "Аккаунт";
    const email = account.profileEmail || "";
    return email ? `${name} (${email})` : name;
  };

  const getCategoryLabel = (category) => {
    if (!category) return "";
    const raw =
      category.name ||
      category.label ||
      category.title ||
      category.categoryName ||
      category.displayName;
    const text = String(raw || "").trim();
    if (text) return text;
    const fallback = category.id || category.url || "";
    return String(fallback);
  };

  const isNumericCategoryId = (value) => /^\d+$/.test(String(value || ""));

  const looksLikeCategoryUrl = (value) => /\/c\d+(?:\/|$)/i.test(String(value || ""));

  const isAttributeOptionNode = (node, parent) => {
    if (!node || !parent) return false;
    const parentId = parent.id !== undefined && parent.id !== null ? String(parent.id) : "";
    if (!isNumericCategoryId(parentId)) return false;
    const idValue = node.id !== undefined && node.id !== null ? String(node.id) : "";
    const urlValue = node.url ? String(node.url) : "";
    const nameValue = String(node.name || node.label || "").trim().toLowerCase();
    const normalizedId = idValue.trim().toLowerCase();
    if (isNumericCategoryId(normalizedId)) return false;
    if (looksLikeCategoryUrl(urlValue)) return false;
    if (Array.isArray(node.children) && node.children.length > 0) return false;
    if (!normalizedId || !nameValue) return false;
    // Keep real subcategories with slug-like ids (e.g. "drucker_scanner").
    // Filter only very small option-like values that are likely field options.
    const optionLikeValues = new Set(["ja", "nein", "yes", "no", "new", "used", "other"]);
    return optionLikeValues.has(normalizedId) || optionLikeValues.has(nameValue);
  };

  const filterCategoryChildren = (nodes, parent) =>
    (Array.isArray(nodes) ? nodes : []).filter((node) => !isAttributeOptionNode(node, parent));

  const getCategoryValue = (category) => {
    if (!category) return "";
    const id = category.id !== undefined && category.id !== null ? String(category.id) : "";
    const url = category.url ? String(category.url) : "";
    if (id && isNumericCategoryId(id)) return id;
    if (url) return url;
    return id;
  };

  const getNodeKey = (node) => {
    if (!node) return "";
    const value = getCategoryValue(node);
    if (value) return `value:${value}`;
    if (node.name) return `name:${String(node.name)}`;
    return "";
  };

  const mergeCategoryTrees = (prevNodes, nextNodes) => {
    if (!Array.isArray(nextNodes) || !nextNodes.length) {
      return Array.isArray(prevNodes) ? prevNodes : [];
    }
    const prev = Array.isArray(prevNodes) ? prevNodes : [];
    const prevMap = new Map();
    for (const node of prev) {
      const key = getNodeKey(node);
      if (key) prevMap.set(key, node);
    }
    const used = new Set();
    const merged = nextNodes.map((node) => {
      const key = getNodeKey(node);
      if (key) used.add(key);
      const prevNode = key ? prevMap.get(key) : null;
      const nextChildren = Array.isArray(node.children) ? node.children : [];
      const prevChildren = prevNode?.children || [];
      const mergedChildren = nextChildren.length
        ? mergeCategoryTrees(prevChildren, nextChildren)
        : prevChildren;
      const childrenLoaded = Boolean(node?.childrenLoaded || prevNode?.childrenLoaded);
      return {
        ...(prevNode || {}),
        ...node,
        children: mergedChildren,
        childrenLoaded
      };
    });
    for (const [key, node] of prevMap.entries()) {
      if (!used.has(key)) merged.push(node);
    }
    return merged;
  };

  const buildPathToNode = (nodes, targetValue, path = []) => {
    if (!targetValue || !Array.isArray(nodes)) return [];
    const target = String(targetValue);
    for (const node of nodes) {
      const matches =
        (node.id !== undefined && node.id !== null && String(node.id) === target) ||
        (node.url && String(node.url) === target);
      const nextValue = getCategoryValue(node);
      const nextPath = nextValue ? [...path, nextValue] : path;
      if (matches) return nextPath;
      if (node.children?.length) {
        const childPath = buildPathToNode(node.children, targetValue, nextPath);
        if (childPath.length) return childPath;
      }
    }
    return [];
  };

  const findNode = (nodes, value) => {
    if (!value) return null;
    for (const node of nodes) {
      if (String(node.id) === String(value) || String(node.url) === String(value)) return node;
      if (node.children?.length) {
        const found = findNode(node.children, value);
        if (found) return found;
      }
    }
    return null;
  };

  const getBreadcrumbForPath = (path) => {
    const breadcrumb = [];
    let current = localCategories;
    for (const pathItem of path) {
      const found = current.find((cat) =>
        (cat.id && String(cat.id) === String(pathItem)) ||
        (cat.url && String(cat.url) === String(pathItem))
      );
      if (!found) break;
      breadcrumb.push(found);
      current = filterCategoryChildren(found.children || [], found);
    }
    return breadcrumb;
  };

  const resolveCategorySelection = (path) => {
    const breadcrumb = getBreadcrumbForPath(path);
    const selectedNode = breadcrumb.length ? breadcrumb[breadcrumb.length - 1] : null;
    const numericNode = [...breadcrumb].reverse().find((node) => /^\d+$/.test(String(node?.id || "")));
    return {
      selectedNode,
      numericNode: numericNode || selectedNode
    };
  };

  useEffect(() => {
    setLocalCategories((prev) => mergeCategoryTrees(prev, categories || []));
  }, [categories]);

  useEffect(() => {
    if (isOpen) {
      setCategoryPath([]);
      setDidRestorePath(false);
      setCategoryChildrenError("");
      setNewAd((prev) => ({
        ...prev,
        categoryId: "",
        categoryUrl: "",
        categoryPath: []
      }));
    }
  }, [isOpen, setNewAd]);

  useEffect(() => {
    if (!adImages || adImages.length === 0) {
      setImagePreviews([]);
      return undefined;
    }

    const previews = adImages.map((file) => ({
      file,
      url: URL.createObjectURL(file)
    }));
    setImagePreviews(previews);

    return () => {
      previews.forEach((preview) => URL.revokeObjectURL(preview.url));
    };
  }, [adImages]);

  const stopRightDrag = () => {
    if (!rightDragRef.current.active) return;
    rightDragRef.current = { active: false, index: -1 };
    setRightDragActive(false);
    setRightDragIndex(null);
  };

  useEffect(() => {
    const handleMouseUp = () => stopRightDrag();
    const handleWindowBlur = () => stopRightDrag();
    window.addEventListener("mouseup", handleMouseUp);
    window.addEventListener("blur", handleWindowBlur);
    return () => {
      window.removeEventListener("mouseup", handleMouseUp);
      window.removeEventListener("blur", handleWindowBlur);
    };
  }, []);

  const handleImageInputChange = (event) => {
    const selected = Array.from(event?.target?.files || []).filter((file) =>
      String(file?.type || "").toLowerCase().startsWith("image/")
    );
    if (event?.target) event.target.value = "";
    if (!selected.length) return;
    setAdImages(selected);
  };

  const handleImageDrop = (event) => {
    event.preventDefault();
    const selected = Array.from(event?.dataTransfer?.files || []).filter((file) =>
      String(file?.type || "").toLowerCase().startsWith("image/")
    );
    if (!selected.length) return;
    setAdImages(selected);
  };

  const removeAdImage = (indexToRemove) => {
    stopRightDrag();
    setAdImages((prev) => {
      const list = Array.isArray(prev) ? prev : [];
      return list.filter((_, idx) => idx !== indexToRemove);
    });
    if (imageInputRef.current) {
      imageInputRef.current.value = "";
    }
  };

  const startRightDrag = (event, index) => {
    if (event.button !== 2) return;
    event.preventDefault();
    rightDragRef.current = { active: true, index };
    setRightDragActive(true);
    setRightDragIndex(index);
  };

  const moveRightDragTo = (targetIndex) => {
    const current = rightDragRef.current;
    if (!current.active) return;
    if (!Number.isInteger(targetIndex)) return;
    if (current.index === targetIndex) return;

    setAdImages((prev) => {
      const list = Array.isArray(prev) ? [...prev] : [];
      if (
        current.index < 0 ||
        current.index >= list.length ||
        targetIndex < 0 ||
        targetIndex >= list.length
      ) {
        return prev;
      }
      const [moved] = list.splice(current.index, 1);
      list.splice(targetIndex, 0, moved);
      return list;
    });

    rightDragRef.current = { active: true, index: targetIndex };
    setRightDragIndex(targetIndex);
  };

  useEffect(() => {
    if (!isOpen) return;
    setNewAd((prev) => {
      const currentPath = Array.isArray(prev.categoryPath) ? prev.categoryPath : [];
      const nextPath = categoryPath;
      if (currentPath.length === nextPath.length && currentPath.every((item, idx) => String(item) === String(nextPath[idx]))) {
        return prev;
      }
      return { ...prev, categoryPath: [...nextPath] };
    });
  }, [isOpen, categoryPath, setNewAd]);

  useEffect(() => {
    if (!isOpen) return;
    if (categoryPath.length) return;
    if (didRestorePath) return;
    const target = newAd?.categoryId || newAd?.categoryUrl;
    if (!target) return;
    const restoredPath = buildPathToNode(localCategories, target);
    if (restoredPath.length) {
      setCategoryPath(restoredPath);
      setDidRestorePath(true);
    }
  }, [isOpen, categoryPath.length, didRestorePath, newAd?.categoryId, newAd?.categoryUrl, localCategories]);

  useEffect(() => {
    if (!isOpen) return;
    if (!newAd.accountId) return;
    if (!categoryPath.length) return;
    const lastId = categoryPath[categoryPath.length - 1];
    const node = findNode(localCategories, lastId);
    if (node && (!node.children || node.children.length === 0)) {
      requestChildren(node);
    }
  }, [isOpen, newAd.accountId, categoryPath, localCategories]);

  useEffect(() => {
    if (!isOpen) return;
    if (!categoryPath.length) {
      console.log("[AdModal] category path empty");
      return;
    }
    const current = getCurrentCategories();
    console.log("[AdModal] category path", categoryPath.join(" > "), "current count", current.length);
  }, [isOpen, categoryPath, localCategories]);

  useEffect(() => {
    if (!isOpen) return;
    if (!categoryListRef.current) return;
    categoryListRef.current.scrollTop = 0;
  }, [isOpen, categoryPath]);

  const getCurrentCategories = () => {
    if (categoryPath.length === 0) return localCategories;

    let current = localCategories;
    let parent = null;
    for (const pathItem of categoryPath) {
      const found = current.find((cat) =>
        (cat.id && String(cat.id) === String(pathItem)) ||
        (cat.url && String(cat.url) === String(pathItem))
      );
      if (!found) {
        const fallback = findNode(localCategories, pathItem);
        return filterCategoryChildren(fallback?.children || [], fallback);
      }
      parent = found;
      current = filterCategoryChildren(found.children || [], found);
    }
    return current;
  };

  const getSelectedCategory = () => {
    if (categoryPath.length === 0) return null;
    const lastId = categoryPath[categoryPath.length - 1];
    return findNode(localCategories, lastId);
  };

  const updateCategorySelection = async (category) => {
    if (!newAd.accountId) {
      alert("Сначала выберите аккаунт");
      return;
    }
    setDidRestorePath(true);
    const parentNode = getSelectedCategory();
    if (isAttributeOptionNode(category, parentNode)) {
      console.warn("[AdModal] ignoring attribute option node", category?.name || category?.id);
      return;
    }
    const value = getCategoryValue(category);
    if (!value) return;
    const normalizedValue = String(value);

    // Если категория уже в пути, возвращаемся к ней
    const indexInPath = categoryPath.findIndex((id) => String(id) === normalizedValue);
    if (indexInPath !== -1) {
      const nextPath = categoryPath.slice(0, indexInPath + 1);
      setCategoryPath(nextPath);

      // Обновляем выбранную категорию
      const { selectedNode, numericNode } = resolveCategorySelection(nextPath);
      setNewAd((prev) => ({
        ...prev,
        categoryId: numericNode?.id ? String(numericNode.id) : "",
        categoryUrl: selectedNode?.url || numericNode?.url || "",
        categoryKey: nextPath.join(">"),
        categoryPath: [...nextPath]
      }));
      return;
    }

    // Добавляем категорию в путь
    const newPath = [...categoryPath, normalizedValue];
    setCategoryPath(newPath);
    setCategoryChildrenError("");

    const knownChildren = Array.isArray(category?.children)
      ? filterCategoryChildren(category.children, category)
      : [];
    const childrenAlreadyLoaded = Boolean(category?.childrenLoaded);
    const shouldFetchChildren = knownChildren.length === 0 && !childrenAlreadyLoaded;
    let childrenResult;
    let requestAttempted = false;
    if (shouldFetchChildren) {
      requestAttempted = true;
      childrenResult = await requestChildren(category);
    }

    // Обновляем выбранную категорию в форме только если она конечная
    const { selectedNode, numericNode } = resolveCategorySelection(newPath);
    const selectedChildren = Array.isArray(selectedNode?.children) ? selectedNode.children : [];
    const fetchedChildren = Array.isArray(childrenResult) ? childrenResult : null;
    const hasFetchedChildren = fetchedChildren
      ? filterCategoryChildren(fetchedChildren, category).length > 0
      : null;
    const hasKnownChildren =
      knownChildren.length > 0 ||
      filterCategoryChildren(selectedChildren, selectedNode).length > 0;
    const childrenLoadFailed = requestAttempted && childrenResult === null;
    const isLeaf = childrenLoadFailed
      ? false
      : (hasFetchedChildren !== null ? !hasFetchedChildren : !hasKnownChildren);

    setNewAd((prev) => {
      const next = { ...prev };
      if (isLeaf) {
        next.categoryId = numericNode?.id ? String(numericNode.id) : "";
        next.categoryUrl = selectedNode?.url || numericNode?.url || "";
      } else {
        next.categoryId = "";
        next.categoryUrl = "";
      }
      next.categoryKey = newPath.join(">");
      next.categoryPath = [...newPath];

      if (!next.categoryId || next.categoryId !== prev.categoryId) {
        if (setExtraFieldValues) {
          setExtraFieldValues({});
        }
      }
      return next;
    });
  };

  const updateNodeChildren = (nodes, targetValue, children) =>
    nodes.map((node) => {
      const matches = String(node.id) === String(targetValue) || String(node.url) === String(targetValue);
      if (matches) {
        return { ...node, children, childrenLoaded: true };
      }
      if (node.children?.length) {
        return { ...node, children: updateNodeChildren(node.children, targetValue, children) };
      }
      return node;
    });

  const requestChildren = async (node) => {
    const rawId = node?.id !== undefined && node?.id !== null ? String(node.id) : "";
    const rawUrl = node?.url ? String(node.url) : "";
    const idValue = rawId && isNumericCategoryId(rawId) ? rawId : "";
    const urlValue = idValue ? "" : (rawUrl || "");
    if (!idValue && !urlValue) return;
    const targetValue = getCategoryValue(node) || idValue || urlValue;
    if (!targetValue) return;
    if (node?.childrenLoaded && Array.isArray(node?.children)) {
      return node.children;
    }

    const pending = pendingChildrenRef.current.get(targetValue);
    if (pending) return pending;

    const task = (async () => {
      const params = new URLSearchParams();
      if (idValue) params.set("id", idValue);
      else params.set("url", urlValue);
      if (newAd?.accountId) params.set("accountId", newAd.accountId);
      try {
        const data = await apiFetchJson(`/api/categories/children?${params.toString()}`, {
          timeoutMs: 30000,
          retry: true,
          allowBaseFallback: true
        });
        const rawChildren = Array.isArray(data?.children) ? data.children : null;
        if (!Array.isArray(rawChildren)) return null;
        const filteredChildren = filterCategoryChildren(rawChildren, node);
        const count = Array.isArray(filteredChildren) ? filteredChildren.length : 0;
        const sample = Array.isArray(filteredChildren)
          ? filteredChildren.slice(0, 5).map((item) => item?.name || item?.id || item?.url)
          : [];
        console.log("[AdModal] children response count", count, "target", targetValue, "sample", sample);
        setLocalCategories((prev) => {
          const next = updateNodeChildren(prev, targetValue, filteredChildren);
          const updated = findNode(next, targetValue);
          console.log("[AdModal] children applied count", updated?.children?.length || 0, "target", targetValue);
          return next;
        });
        setCategoryChildrenError("");
        return filteredChildren;
      } catch (error) {
        const message = error?.message || "Ошибка сети";
        console.error("[AdModal] children request failed", {
          target: targetValue,
          accountId: newAd?.accountId || "",
          message
        });
        setCategoryChildrenError(`Не удалось загрузить подкатегории: ${message}`);
        return null;
      }
    })();

    pendingChildrenRef.current.set(targetValue, task);
    try {
      return await task;
    } finally {
      pendingChildrenRef.current.delete(targetValue);
    }
  };

  const fetchAccountPostalCode = async () => {
    if (!newAd?.accountId) {
      alert("Сначала выберите аккаунт");
      return;
    }
    setLoadingPlz(true);
    try {
      const data = await apiFetchJson(`/api/accounts/${newAd.accountId}/plz`);
      if (data?.success && data?.postalCode) {
        setNewAd((prev) => ({ ...prev, postalCode: data.postalCode }));
      } else {
        alert(data?.error || "Не удалось получить PLZ аккаунта");
      }
    } catch (error) {
      alert(error?.message || "Не удалось получить PLZ аккаунта");
    } finally {
      setLoadingPlz(false);
    }
  };

  useEffect(() => {
    if (!isOpen) return;
    if (!newAd.accountId) return;
    if (categoryPath.length !== 1) return;
    const rootValue = String(categoryPath[0] || "");
    if (!rootValue) return;

    if (prefetchRootRef.current !== rootValue) {
      prefetchRootRef.current = rootValue;
      prefetchedTargetsRef.current = new Set();
      prefetchInFlightRef.current = false;
    }

    if (prefetchInFlightRef.current) return;
    const current = getCurrentCategories();
    if (!current.length) return;

    const token = ++prefetchTokenRef.current;
    prefetchInFlightRef.current = true;

    (async () => {
      for (const node of current) {
        if (prefetchTokenRef.current !== token) return;
        const key = getCategoryValue(node);
        if (!key) continue;
        if (prefetchedTargetsRef.current.has(key)) continue;
        prefetchedTargetsRef.current.add(key);
        if (node.children?.length || node.childrenLoaded) continue;
        await requestChildren(node);
      }
    })().finally(() => {
      if (prefetchTokenRef.current === token) {
        prefetchInFlightRef.current = false;
      }
    });
  }, [isOpen, newAd.accountId, categoryPath, localCategories]);

  const getCategoryBreadcrumb = () => {
    const breadcrumb = [];
    let current = localCategories;

    for (const pathItem of categoryPath) {
      const found = current.find((cat) =>
        (cat.id && String(cat.id) === String(pathItem)) ||
        (cat.url && String(cat.url) === String(pathItem))
      );
      if (found) {
        breadcrumb.push(found);
        current = filterCategoryChildren(found.children || [], found);
      }
    }

    return breadcrumb;
  };

  const goBackInCategories = (index) => {
    setDidRestorePath(true);
    if (index < 0) {
      setCategoryPath([]);
    } else {
      setCategoryPath(categoryPath.slice(0, index + 1));
    }
  };

  const currentCategories = getCurrentCategories();
  const breadcrumb = getCategoryBreadcrumb();

  return (
    <div className="modal-overlay" style={{
      position: "fixed",
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundColor: "rgba(2, 6, 23, 0.85)",
      backdropFilter: "blur(8px)",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      zIndex: 5000,
      padding: "20px",
      animation: "fadeIn 0.2s ease-out"
    }}>
      <div className="modal-card modal-card-wide modal-card-tall" style={{
        background: "linear-gradient(145deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.98) 100%)",
        borderRadius: "24px",
        padding: "32px",
        width: "760px",
        maxWidth: "100%",
        maxHeight: "92vh",
        overflow: "auto",
        border: "1px solid rgba(148,163,184,0.15)",
        boxShadow: "0 30px 60px rgba(0,0,0,0.5), 0 0 60px rgba(0,0,0,0.3)",
        color: "#e2e8f0",
        animation: "scaleIn 0.3s ease-out"
      }}>
        <div style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
          marginBottom: "28px"
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: "14px" }}>
            <div style={{
              width: "48px",
              height: "48px",
              borderRadius: "14px",
              background: "linear-gradient(135deg, rgba(245, 158, 11, 0.2), rgba(245, 158, 11, 0.1))",
              border: "1px solid rgba(245, 158, 11, 0.3)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              boxShadow: "0 4px 15px rgba(245, 158, 11, 0.2)",
              fontSize: "22px"
            }}>
              📦
            </div>
            <div>
              <h2 style={{ margin: 0, fontSize: "22px", fontWeight: "700" }}>Создать объявление</h2>
              <p style={{ margin: "4px 0 0", color: "#94a3b8", fontSize: "13px" }}>
                Заполните поля и опубликуйте в Kleinanzeigen
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              width: "40px",
              height: "40px",
              borderRadius: "12px",
              background: "rgba(148,163,184,0.1)",
              border: "1px solid rgba(148,163,184,0.2)",
              cursor: "pointer",
              color: "#94a3b8",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              transition: "all 0.2s ease"
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "rgba(239, 68, 68, 0.15)";
              e.currentTarget.style.borderColor = "rgba(239, 68, 68, 0.3)";
              e.currentTarget.style.color = "#f87171";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "rgba(148,163,184,0.1)";
              e.currentTarget.style.borderColor = "rgba(148,163,184,0.2)";
              e.currentTarget.style.color = "#94a3b8";
            }}
          >
            <XIcon size={18} />
          </button>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: "18px" }}>
          <div>
            <label style={labelStyle}>Аккаунт:</label>
            <select
              value={newAd.accountId}
              onChange={(e) => setNewAd((prev) => ({ ...prev, accountId: e.target.value }))}
              style={inputStyle}
            >
              <option value="">Выберите аккаунт</option>
              {accounts.map((account) => (
                <option key={account.id} value={account.id}>
                  {formatAccountLabel(account)} ({account.proxy || "Без прокси"})
                </option>
              ))}
            </select>
          </div>

          <div>
            <label style={labelStyle}>Название объявления:</label>
            <input
              type="text"
              value={newAd.title}
              onChange={(e) => setNewAd((prev) => ({ ...prev, title: e.target.value }))}
              placeholder="Введите название (10-65 символов)"
              style={inputStyle}
              onFocus={(e) => e.currentTarget.style.borderColor = "rgba(59,130,246,0.5)"}
              onBlur={(e) => e.currentTarget.style.borderColor = "rgba(148,163,184,0.3)"}
            />
          </div>

          <div>
            <label style={labelStyle}>Описание:</label>
            <textarea
              value={newAd.description}
              onChange={(e) => setNewAd((prev) => ({ ...prev, description: e.target.value }))}
              placeholder="Введите описание (10-4000 символов)"
              rows="5"
              style={{
                ...inputStyle,
                minHeight: "140px",
                resize: "vertical",
                fontFamily: "inherit"
              }}
              onFocus={(e) => e.currentTarget.style.borderColor = "rgba(59,130,246,0.5)"}
              onBlur={(e) => e.currentTarget.style.borderColor = "rgba(148,163,184,0.3)"}
            />
          </div>

          <div>
            <label style={labelStyle}>Цена (EUR):</label>
            <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
              <input
                type="number"
                value={newAd.price}
                onChange={(e) => setNewAd((prev) => ({ ...prev, price: e.target.value }))}
                placeholder="0.00"
                style={inputStyle}
                onFocus={(e) => e.currentTarget.style.borderColor = "rgba(59,130,246,0.5)"}
                onBlur={(e) => e.currentTarget.style.borderColor = "rgba(148,163,184,0.3)"}
              />
              <span style={{ color: "#94a3b8", fontWeight: "600" }}>€</span>
            </div>
          </div>

          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <label style={labelStyle}>Категория:</label>
              <div style={{ display: "flex", gap: "10px", alignItems: "center" }}>
                {categoriesUpdatedAt && (
                  <span style={{ fontSize: "12px", color: "#94a3b8" }}>
                    Обновлено: {new Date(categoriesUpdatedAt).toLocaleDateString()}
                  </span>
                )}
                <button
                  type="button"
                  onClick={() => onRefreshCategories(true)}
                  disabled={loadingCategories}
                  className="secondary-button"
                  style={{
                    padding: "6px 12px",
                    border: "1px solid rgba(148,163,184,0.3)",
                    cursor: loadingCategories ? "not-allowed" : "pointer",
                    fontSize: "12px",
                    display: "flex",
                    alignItems: "center",
                    gap: "6px"
                  }}
                >
                  {loadingCategories ? <LoadingIcon size={14} /> : <RefreshIcon size={14} />}
                  {loadingCategories ? "Обновление..." : "Обновить"}
                </button>
              </div>
            </div>

            <div style={{
              border: "1px solid rgba(148,163,184,0.3)",
              borderRadius: "12px",
              background: "rgba(15,23,42,0.7)",
              marginTop: "10px",
              overflow: "hidden"
            }}>
              <div style={{ padding: "12px 16px", borderBottom: "1px solid rgba(148,163,184,0.2)" }}>
                <div style={{ fontWeight: "600", color: "#e2e8f0" }}>Выберите категорию</div>
                <div style={{ fontSize: "12px", color: "#94a3b8" }}>Навигация по категориям</div>
              </div>

              {/* Хлебные крошки */}
              {breadcrumb.length > 0 && (
                <div style={{
                  padding: "10px 16px",
                  borderBottom: "1px solid rgba(148,163,184,0.2)",
                  display: "flex",
                  gap: "8px",
                  alignItems: "center",
                  flexWrap: "wrap",
                  fontSize: "12px",
                  color: "#94a3b8"
                }}>
                  <button
                    onClick={() => goBackInCategories(-1)}
                    style={{
                      background: "rgba(59,130,246,0.1)",
                      border: "1px solid rgba(59,130,246,0.3)",
                      borderRadius: "6px",
                      padding: "4px 8px",
                      cursor: "pointer",
                      color: "#7dd3fc",
                      fontSize: "12px",
                      transition: "all 0.2s ease"
                    }}
                    onMouseEnter={(e) => e.currentTarget.style.background = "rgba(59,130,246,0.2)"}
                    onMouseLeave={(e) => e.currentTarget.style.background = "rgba(59,130,246,0.1)"}
                  >
                    Все категории
                  </button>
                  {breadcrumb.map((cat, index) => (
                    <React.Fragment key={cat.id || cat.url}>
                      <ChevronRightIcon size={12} />
                      <button
                        onClick={() => goBackInCategories(index)}
                        style={{
                          background: index === breadcrumb.length - 1 ? "rgba(59,130,246,0.2)" : "rgba(148,163,184,0.1)",
                          border: "1px solid " + (index === breadcrumb.length - 1 ? "rgba(59,130,246,0.3)" : "rgba(148,163,184,0.2)"),
                          borderRadius: "6px",
                          padding: "4px 8px",
                          cursor: index === breadcrumb.length - 1 ? "default" : "pointer",
                          color: index === breadcrumb.length - 1 ? "#7dd3fc" : "#e2e8f0",
                          fontSize: "12px",
                          transition: "all 0.2s ease"
                        }}
                      >
                        {getCategoryLabel(cat)}
                      </button>
                    </React.Fragment>
                  ))}
                </div>
              )}

              <div ref={categoryListRef} style={{ padding: "12px", minHeight: "220px", maxHeight: "300px", overflowY: "auto" }}>
                {currentCategories.length ? (
                  currentCategories.map((category) => {
                    return (
                      <button
                        key={category.id || category.url || getCategoryLabel(category)}
                        type="button"
                        onClick={() => updateCategorySelection(category)}
                        disabled={!newAd.accountId}
                        style={{
                          width: "100%",
                          textAlign: "left",
                          padding: "10px 12px",
                          marginBottom: "6px",
                          borderRadius: "10px",
                          border: "1px solid transparent",
                          background: "rgba(148,163,184,0.05)",
                          color: "#e2e8f0",
                          cursor: newAd.accountId ? "pointer" : "not-allowed",
                          transition: "all 0.2s ease"
                        }}
                        onMouseEnter={(e) => {
                          e.currentTarget.style.background = "rgba(59,130,246,0.1)";
                          e.currentTarget.style.borderColor = "rgba(59,130,246,0.3)";
                        }}
                        onMouseLeave={(e) => {
                          e.currentTarget.style.background = "rgba(148,163,184,0.05)";
                          e.currentTarget.style.borderColor = "transparent";
                        }}
                      >
                        <span style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                          <span>{getCategoryLabel(category)}</span>
                          <ChevronRightIcon size={16} color="#94a3b8" />
                        </span>
                      </button>
                    );
                  })
                ) : (
                  <div style={{ color: "#94a3b8", padding: "12px", textAlign: "center" }}>
                    {!newAd.accountId
                      ? "Сначала выберите аккаунт"
                      : categoryChildrenError
                        ? "Не удалось загрузить подкатегории. Нажмите «Обновить»."
                      : categoryPath.length > 0
                        ? "Нет подкатегорий - эта категория финальная"
                        : "Категории не найдены"}
                  </div>
                )}
              </div>

              {categoryChildrenError && (
                <div style={{ color: "#f87171", fontSize: "12px", padding: "0 16px 10px" }}>
                  {categoryChildrenError}
                </div>
              )}

              <div style={{
                borderTop: "1px solid rgba(148,163,184,0.2)",
                padding: "10px 16px",
                color: "#94a3b8",
                fontSize: "12px",
                display: "flex",
                gap: "8px",
                flexWrap: "wrap",
                alignItems: "center"
              }}>
                <span style={{
                  background: "rgba(59,130,246,0.2)",
                  padding: "4px 8px",
                  borderRadius: "999px",
                  color: "#e2e8f0",
                  fontSize: "11px",
                  fontWeight: "600"
                }}>
                  Выбрано
                </span>
                {breadcrumb.length > 0 ? (
                  breadcrumb.map((cat, index) => (
                    <React.Fragment key={cat.id || cat.url}>
                      {index > 0 && <ChevronRightIcon size={12} />}
                      <span style={index === breadcrumb.length - 1 ? selectedUnderlineStyle : undefined}>
                        {getCategoryLabel(cat)}
                      </span>
                    </React.Fragment>
                  ))
                ) : (
                  <span>—</span>
                )}
              </div>
            </div>
          </div>

          {loadingExtraFields && (
            <div style={{ color: "#94a3b8", fontSize: "12px", display: "flex", alignItems: "center", gap: "8px" }}>
              <LoadingIcon size={14} />
              Загрузка параметров категории...
            </div>
          )}

          {!loadingExtraFields && extraFieldsError && (
            <div style={{ color: "#f87171", fontSize: "12px" }}>
              Не удалось загрузить параметры категории: {extraFieldsError}
            </div>
          )}

          {!loadingExtraFields && !extraFieldsError && (newAd.categoryId || newAd.categoryUrl) && (!extraFields || extraFields.length === 0) && (
            <div style={{ color: "#94a3b8", fontSize: "12px" }}>
              Для выбранной категории нет дополнительных параметров.
            </div>
          )}

          {!loadingExtraFields && extraFields?.length > 0 && (
            <div style={{ marginTop: "6px" }}>
              <label style={labelStyle}>Дополнительные параметры:</label>
              <div style={{ display: "grid", gap: "12px" }}>
                {extraFields.map((field) => {
                  const key = field.name || field.label;
                  const value = key ? (extraFieldValues?.[key] || "") : "";
                  return (
                    <div key={field.name || field.label} style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
                      <span style={{ fontSize: "12px", color: "#94a3b8" }}>
                        {field.label}{field.required ? " *" : ""}
                      </span>
                      <select
                        value={value}
                        onChange={(e) => {
                          const nextValue = e.target.value;
                          setExtraFieldValues((prev) => ({ ...prev, [key]: nextValue }));
                        }}
                        style={inputStyle}
                      >
                        <option value="">Выберите</option>
                        {field.options?.map((option) => (
                          <option key={option.value || option.label} value={option.value}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                    </div>
                  );
                })}
              </div>
              {missingRequiredExtraFields.length > 0 && (
                <div style={{ marginTop: "10px", color: "#fbbf24", fontSize: "12px" }}>
                  Заполните обязательные параметры, отмеченные звездочкой (*).
                </div>
              )}
            </div>
          )}

          <div>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "12px" }}>
              <label style={labelStyle}>PLZ (индекс):</label>
              <button
                type="button"
                onClick={fetchAccountPostalCode}
                disabled={loadingPlz}
                style={{
                  padding: "6px 12px",
                  borderRadius: "999px",
                  border: "1px solid rgba(59,130,246,0.35)",
                  background: "rgba(59,130,246,0.1)",
                  color: "#60a5fa",
                  fontSize: "12px",
                  fontWeight: "600",
                  cursor: loadingPlz ? "not-allowed" : "pointer",
                  whiteSpace: "nowrap"
                }}
              >
                {loadingPlz ? "Загрузка..." : "PLZ аккаунта"}
              </button>
            </div>
            <input
              type="text"
              value={newAd.postalCode}
              onChange={(e) => setNewAd((prev) => ({ ...prev, postalCode: e.target.value }))}
              placeholder="Введите почтовый индекс"
              style={inputStyle}
              onFocus={(e) => e.currentTarget.style.borderColor = "rgba(59,130,246,0.5)"}
              onBlur={(e) => e.currentTarget.style.borderColor = "rgba(148,163,184,0.3)"}
            />
          </div>

          <div>
            <label style={labelStyle}>Изображения:</label>
            <div
              style={{
                padding: "28px 24px",
                border: "2px dashed rgba(148,163,184,0.3)",
                borderRadius: "16px",
                background: "rgba(15,23,42,0.6)",
                textAlign: "center",
                cursor: "pointer",
                transition: "all 0.2s ease",
                position: "relative"
              }}
              onDragOver={(event) => {
                event.preventDefault();
                event.currentTarget.style.borderColor = "rgba(59,130,246,0.5)";
                event.currentTarget.style.background = "rgba(59,130,246,0.06)";
              }}
              onDragLeave={(event) => {
                event.currentTarget.style.borderColor = "rgba(148,163,184,0.3)";
                event.currentTarget.style.background = "rgba(15,23,42,0.6)";
              }}
              onDrop={(event) => {
                handleImageDrop(event);
                event.currentTarget.style.borderColor = "rgba(148,163,184,0.3)";
                event.currentTarget.style.background = "rgba(15,23,42,0.6)";
              }}
              onClick={() => imageInputRef.current?.click()}
            >
              <input
                ref={imageInputRef}
                type="file"
                accept="image/*"
                multiple
                onChange={handleImageInputChange}
                style={{ display: "none" }}
              />
              <div style={{
                width: "56px",
                height: "56px",
                borderRadius: "16px",
                background: "linear-gradient(135deg, rgba(59,130,246,0.2), rgba(59,130,246,0.1))",
                border: "1px solid rgba(59,130,246,0.3)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                margin: "0 auto 16px"
              }}>
                <FileIcon size={28} color="#60a5fa" />
              </div>
              <p style={{ margin: 0, color: "#94a3b8", fontSize: "14px" }}>
                Перетащите фото сюда или <span style={{ color: "#60a5fa", fontWeight: "600" }}>выберите</span>
              </p>
              <p style={{ margin: "8px 0 0", color: "#64748b", fontSize: "12px" }}>
                Поддерживаются: JPG, PNG, WEBP, GIF
              </p>
            </div>
            {adImages.length > 0 && (
              <div style={{ marginTop: "8px", fontSize: "12px", color: "#94a3b8" }}>
                Загружено файлов: {adImages.length}. Правой кнопкой мыши зажмите и перетащите для смены порядка.
              </div>
            )}
            {imagePreviews.length > 0 && (
              <div
                style={{
                  marginTop: "12px",
                  display: "grid",
                  gridTemplateColumns: "repeat(auto-fill, minmax(96px, 1fr))",
                  gap: "10px"
                }}
              >
                {imagePreviews.map((preview, index) => (
                  <div
                    key={preview.url}
                    onContextMenu={(event) => event.preventDefault()}
                    onMouseDown={(event) => startRightDrag(event, index)}
                    onMouseEnter={() => moveRightDragTo(index)}
                    onMouseUp={() => stopRightDrag()}
                    style={{
                      position: "relative",
                      borderRadius: "10px",
                      overflow: "hidden",
                      background: "#0b1220",
                      border: rightDragIndex === index
                        ? "1px solid rgba(34,197,94,0.75)"
                        : "1px solid rgba(148,163,184,0.2)",
                      aspectRatio: "1 / 1",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      boxShadow: rightDragIndex === index
                        ? "0 0 0 2px rgba(34,197,94,0.25)"
                        : "none",
                      cursor: rightDragActive ? "grabbing" : "default",
                      userSelect: "none"
                    }}
                  >
                    <img
                      src={preview.url}
                      alt={`preview-${index + 1}`}
                      draggable={false}
                      style={{ width: "100%", height: "100%", objectFit: "cover", pointerEvents: "none" }}
                    />
                    <div style={{
                      position: "absolute",
                      left: "6px",
                      bottom: "6px",
                      minWidth: "22px",
                      height: "22px",
                      padding: "0 6px",
                      borderRadius: "999px",
                      display: "inline-flex",
                      alignItems: "center",
                      justifyContent: "center",
                      fontSize: "11px",
                      fontWeight: 700,
                      color: "#e2e8f0",
                      background: "rgba(2,6,23,0.7)",
                      border: "1px solid rgba(148,163,184,0.25)"
                    }}>
                      {index + 1}
                    </div>
                    <button
                      type="button"
                      onClick={(event) => {
                        event.preventDefault();
                        event.stopPropagation();
                        removeAdImage(index);
                      }}
                      style={{
                        position: "absolute",
                        top: "6px",
                        right: "6px",
                        width: "24px",
                        height: "24px",
                        borderRadius: "999px",
                        border: "1px solid rgba(239,68,68,0.4)",
                        background: "rgba(127,29,29,0.85)",
                        color: "#fecaca",
                        fontSize: "14px",
                        lineHeight: 1,
                        cursor: "pointer",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center"
                      }}
                      title="Удалить фото"
                    >
                      ×
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div style={{ marginTop: "28px", display: "flex", gap: "12px", justifyContent: "flex-end" }}>
            <button
              type="button"
              onClick={onClose}
              className="secondary-button"
              style={{
                padding: "12px 24px",
                border: "none",
                cursor: "pointer",
                borderRadius: "14px",
                fontSize: "14px",
                fontWeight: "600"
              }}
            >
              Отмена
            </button>
            <button
              type="button"
              onClick={onSubmit}
              className="primary-button"
              disabled={publishing || submitBlockedByFields}
              style={{
                padding: "12px 28px",
                color: "white",
                border: "none",
                cursor: (publishing || submitBlockedByFields) ? "not-allowed" : "pointer",
                opacity: (publishing || submitBlockedByFields) ? 0.7 : 1,
                borderRadius: "14px",
                fontSize: "14px",
                fontWeight: "600",
                display: "flex",
                alignItems: "center",
                gap: "8px"
              }}
            >
              {publishing ? "Публикация..." : "📤 Создать объявление"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AdModal;
