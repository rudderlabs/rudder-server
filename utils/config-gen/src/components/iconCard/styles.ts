import styled from 'styled-components';

export const Container = styled.div`
  background: ${({ theme }) => theme.color.white};
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  position: relative;
  height: 120px;
  width: 180px;
  margin: 10px 10px 10px 0px;
  /* Gray 50 */
  border: 1px solid ${({ theme }) => theme.color.grey100};
  border-radius: 10px;
  &:hover {
    border: 1px solid ${({ theme }) => theme.color.primary100};
    /* border-radius: 10px; */
  }
  &.active {
    border: 1px solid ${({ theme }) => theme.color.primary100};
    box-shadow: inset 0 0 6pt 1pt
      ${({ theme }) => theme.color.primary100_transparent};
    background-clip: padding-box;
  }
`;

export const AbsoluteContaier = styled.div`
  position: absolute;
  top: 0px;
  left: 0px;
`;
