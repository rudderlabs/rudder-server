import { Flex } from '@components/common/misc';
import styled from 'styled-components';

export const SourceModalView = styled(Flex).attrs({
  flexDirection: 'row',
  spaceBetween: true,
})`
  background-color: ${({ theme }) => theme.color.white};
  border-radius: 10px;
  padding: 30px 30px 30px 30px;
  box-shadow: 0px 2px 10px rgba(0, 0, 0, 0.15);
`;

export const IconColumn = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
  margin: 0 10px 0 0;
  width: 48px;
  height: 48px;
  border: 2px solid ${({ theme }) => theme.color.grey100};
  border-radius: 50%;
  img {
    max-height: 32px;
    max-width: 32px;
  };
`;

export const SourceNameTitle = styled.div`
  font-size: ${({ theme }) => theme.fontSize.small};
  color: ${({ theme }) => theme.color.grey300};
`;

export const SourceNameText = styled.div`
  font-size: ${({ theme }) => theme.fontSize.h1};
  color: ${({ theme }) => theme.color.black};
`;

export const SourceDetailsSourceId = styled(Flex).attrs({
  flexDirection: 'column',
})`
  margin: 0 50px 0 0;
`;

export const SourceDetailsTitleText = styled.div`
  font-size: ${({ theme }) => theme.fontSize.small};
  color: ${({ theme }) => theme.color.grey300};
  text-align: start;
`;

export const SourceDetailsContentText = styled.div`
    text-align: start
    font-size: ${({ theme }) => theme.fontSize.md};
    word-break: break-word
`;
