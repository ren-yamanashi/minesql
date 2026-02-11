package slottedpage

// Slotted Page のヘッダーサイズ
// ヘッダー情報の内訳は以下の通り
// numSlots: 2 byte (0, 1)
// freeOffset: 2 byte (2, 3)
// pad: 4 byte (4, 5, 6, 7)
const headerSize = 8
