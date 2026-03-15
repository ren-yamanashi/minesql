package slottedpage

// Slotted Page のヘッダーサイズ
// ヘッダー情報の内訳は以下の通り
// numSlots: 2 byte (0, 1) -- スロット数
// freeOffset: 2 byte (2, 3) -- フリースペースの開始位置
// pad: 4 byte (4, 5, 6, 7) -- 予約領域
const headerSize = 8
