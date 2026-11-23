export function configChanges(saved, draft, lockedKeys) {
  return Object.fromEntries(Object.entries(draft).filter(([key, value]) =>
    !lockedKeys.includes(key) && JSON.stringify(value) !== JSON.stringify(saved[key])
  ));
}
